package com.mongodb.socialite.cli;

import com.mongodb.MongoClientURI;
import com.mongodb.socialite.ServiceManager;
import com.mongodb.socialite.SocialiteConfiguration;
import com.mongodb.socialite.api.Content;
import com.mongodb.socialite.api.User;
import com.mongodb.socialite.benchmark.graph.GraphGenerator;
import com.mongodb.socialite.benchmark.graph.GraphMutation;
import com.mongodb.socialite.benchmark.graph.ZipZipfGraphGenerator;
import com.mongodb.socialite.services.ContentService;
import com.mongodb.socialite.services.FeedService;
import com.mongodb.socialite.services.UserGraphService;
import com.mongodb.socialite.util.BlockingWorkQueue;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.lang.Runnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadCommand extends ConfiguredCommand<SocialiteConfiguration> {

	private static Logger logger = LoggerFactory.getLogger(LoadCommand.class);

	public LoadCommand() {
        super("load", "Loads synthetic data for testing");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);    //To change body of overridden methods use File | Settings | File Templates.
        subparser.addArgument("--users").required(true).type(Integer.class);
        subparser.addArgument("--maxfollows").required(true).type(Integer.class);
        subparser.addArgument("--messages").required(true).type(Integer.class);
        subparser.addArgument("--threads").required(true).type(Integer.class);
        subparser.addArgument("--csfle").required(true).type(Integer.class);
    }

    @Override
    protected void run(Bootstrap<SocialiteConfiguration> configBootstrap, Namespace namespace, SocialiteConfiguration config) throws Exception {

        // Get the configured default MongoDB URI
        MongoClientURI default_uri = config.mongodb.default_database_uri;
        
        // Initialize the services as per configuration
        ServiceManager services = new ServiceManager(config.services, default_uri);
        final UserGraphService userGraph = services.getUserGraphService();
        final FeedService feedService = services.getFeedService();
        final ContentService contentService = services.getContentService();

        final int threads = namespace.getInt("threads");

        ExecutorService executor = new ThreadPoolExecutor(threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new BlockingWorkQueue<Runnable>(1000));


        final int userCount = namespace.getInt("users");
        final int maxFollows = namespace.getInt("maxfollows");
        GraphGenerator graphGenerator = new ZipZipfGraphGenerator(maxFollows);

        logger.info("Queuing user graph actions for {} users", userCount);
        for( int i = 0; i < userCount; i++ ) {
            final GraphMutation mutation = graphGenerator.next();
            executor.submit( new Runnable() {
                @Override
                public void run() {
                	try{
	                    userGraph.createUser(mutation.user);
	                    List<User> followers = mutation.getFollowers();
	                    for( User u : followers ) {
	                       userGraph.follow(mutation.user, u);
	                    }
	                    
	                    if(logger.isDebugEnabled()){
	                    	logger.debug("Added {} followers for user {}", 
	                    			followers.size(), mutation.user.getUserId());
	                    }
                	} catch(Exception e){
                		logger.error(e.toString());
                		logger.debug("", e);                		
                	}
                }
            });
        }

        int messageCount = namespace.getInt("messages");
        logger.info("Queuing {} messages for {} users", messageCount, userCount);
        int csfleEnabled = namespace.getInt("csfle");
        if (csfleEnabled == 1)
            logger.info("CSFLE Enabled");
        else
            logger.info("CSFLE Disabled");
        // send messageCount messages from each user
        for( int j = 0; j < messageCount; j++ ) {
            executor.submit( new Runnable() {
                @Override
                public void run() {
                	try{
	                    for( int i = 0; i < userCount; i++ ) {
	                        final User user = new User( String.valueOf(i));
	                        final Content content = new Content( user, randomString(), null);
                                if (csfleEnabled == 1)
                                    content = Encrypt(content);
	                        contentService.publishContent(user, content);
	                        feedService.post( user, content );
	                    }
                	} catch(Exception e){
                		logger.error(e.toString());
                		logger.debug("", e);                		
                	}
                }
            });
        }

        executor.shutdown();
        logger.info("All actions queued, waiting for completion...");
        executor.awaitTermination(Long.MAX_VALUE,TimeUnit.SECONDS);
        logger.info("Done.");
        services.stop();
    }

    private static final char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private Random random = new Random();
    protected String randomString() {

        int length = Math.abs(10 + random.nextInt(130));

        StringBuilder sb = new StringBuilder();
        for( int i = 0; i < length; i++ ) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

    private static final String algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";
    protected Content encrypt(Content c) {
        // Use a hard coded local key since it needs to be shared between load and run phases
        byte[] localMasterKey = new byte[]{0x77, 0x1f, 0x2d, 0x7d, 0x76, 0x74, 0x39, 0x08, 0x50, 0x0b, 0x61, 0x14,
            0x3a, 0x07, 0x24, 0x7c, 0x37, 0x7b, 0x60, 0x0f, 0x09, 0x11, 0x23, 0x65,
            0x35, 0x01, 0x3a, 0x76, 0x5f, 0x3e, 0x4b, 0x6a, 0x65, 0x77, 0x21, 0x6d,
            0x34, 0x13, 0x24, 0x1b, 0x47, 0x73, 0x21, 0x5d, 0x56, 0x6a, 0x38, 0x30,
            0x6d, 0x5e, 0x79, 0x1b, 0x25, 0x4d, 0x2a, 0x00, 0x7c, 0x0b, 0x65, 0x1d,
            0x70, 0x22, 0x22, 0x61, 0x2e, 0x6a, 0x52, 0x46, 0x6a, 0x43, 0x43, 0x23,
            0x58, 0x21, 0x78, 0x59, 0x64, 0x35, 0x5c, 0x23, 0x00, 0x27, 0x43, 0x7d,
            0x50, 0x13, 0x65, 0x3c, 0x54, 0x1e, 0x74, 0x3c, 0x3b, 0x57, 0x21, 0x1a};

        Map<String, Map<String, Object>> kmsProviders =
            Collections.singletonMap("local", Collections.singletonMap("key", localMasterKey));

        // Use the same database, admin is slow
        String keyVaultNamespace = database + ".datakeys";
        String keyVaultUrls = url;
        if (!keyVaultUrls.startsWith("mongodb")) {
            keyVaultUrls = "mongodb://" + keyVaultUrls;
        }

        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
        .keyVaultMongoClientSettings(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(keyVaultUrls))
                .readPreference(readPreference)
                .writeConcern(writeConcern)
                .build())
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .build();

        ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        MongoClient vaultClient = MongoClients.create(keyVaultUrls);

        final MongoCollection<Document> keyCollection = vaultClient.getDatabase(database).getCollection(keyVaultNamespace);

        String base64DataKeyId = getDataKeyOrCreate(keyCollection, clientEncryption);

        String collName = "usertable";
        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
            .keyVaultNamespace(keyVaultNamespace)
            .extraOptions(Collections.singletonMap("mongocryptdBypassSpawn", true) )
            .kmsProviders(kmsProviders);

        autoEncryptionSettingsBuilder.schemaMap(Collections.singletonMap(database + "." + collName,
            // Need a schema that references the new data key
            BsonDocument.parse(generateSchema(base64DataKeyId, numFields))
        ));

        AutoEncryptionSettings autoEncryptionSettings = autoEncryptionSettingsBuilder.build();

        if (remote_schema) {
            com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(keyVaultUrls);
            CreateCollectionOptions options = new CreateCollectionOptions();
            options.getValidationOptions().validator(BsonDocument.parse(generateRemoteSchema(base64DataKeyId, numFields)));
            try {
                client.getDatabase(database).createCollection(collName,  options);
            } catch (com.mongodb.MongoCommandException e) {
                // if this is load phase, then should error, if it's run then should ignore
                // how to tell properly?
                if (client.getDatabase(database).getCollection(collName).estimatedDocumentCount() <= 0) {
                   System.err.println("ERROR: Failed to create collection " + collName + " with error " + e);
                   e.printStackTrace();
                   System.exit(1);
                }
            }
        }

        return autoEncryptionSettings;
    }

}