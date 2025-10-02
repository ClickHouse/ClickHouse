#include <gtest/gtest.h>
#include <config.h>

#if USE_AZURE_BLOB_STORAGE

#include <azure/identity/managed_identity_credential.hpp>

#define ASSERT_THROW_ERROR_CODE(statement, expected_exception, expected_code, expected_msg)     \
    ASSERT_THROW(                                                                               \
        try                                                                                     \
        {                                                                                       \
            (void)(statement);                                                                  \
        }                                                                                       \
        catch (const expected_exception & e)                                                    \
        {                                                                                       \
            EXPECT_EQ(expected_code, e.code());                                                 \
            EXPECT_NE(e.message().find(expected_msg), std::string::npos);                      \
            throw;                                                                              \
        }                                                                                       \
    , expected_exception)

#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Interpreters/Context.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// A class which allows to test private methods of NamedCollectionFactory.
class NamedCollectionFactoryFriend : public NamedCollectionFactory
{
public:
    static NamedCollectionFactoryFriend & instance()
    {
        static NamedCollectionFactoryFriend instance;
        return instance;
    }

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config)
    {
        std::lock_guard lock(mutex);
        NamedCollectionFactory::loadFromConfig(config, lock);
    }
};

class StorageAzureConfigurationFriend : public StorageAzureConfiguration
{
public:
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override
    {
        StorageAzureConfiguration::fromNamedCollection(collection, context);
    }

    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override
    {
        StorageAzureConfiguration::fromAST(args, context, with_structure);
    }

    const AzureBlobStorage::ConnectionParams & getConnectionParams()
    {
        return connection_params;
    }
};

void loadNamedCollectionConfig(const String & xml)
{
    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);

    NamedCollectionFactoryFriend::instance().loadFromConfig(*config);
}

TEST(StorageAzureConfiguration, FromNamedCollectionWithExtraCredentials)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <FromNamedCollectionWithExtraCredentials>
            <container>test_container</container>
            <blob_path>test_blob.csv</blob_path>
            <client_id>test_client_id</client_id>
            <tenant_id>test_tenant_id</tenant_id>
        </FromNamedCollectionWithExtraCredentials>
    </named_collections>
    </clickhouse>)CONFIG");

    loadNamedCollectionConfig(xml);

    StorageAzureConfigurationFriend conf;
    auto collection = NamedCollectionFactoryFriend::instance().get("FromNamedCollectionWithExtraCredentials");
    conf.fromNamedCollection(*collection, Context::getGlobalContextInstance());

    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Azure::Identity::WorkloadIdentityCredential>>(conf.getConnectionParams().auth_method));
}

TEST(StorageAzureConfiguration, FromNamedCollectionWithAccount)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <FromNamedCollectionWithAccount>
            <container>test_container</container>
            <blob_path>test_blob.csv</blob_path>
            <account_name>test_account</account_name>
            <account_key>test_key</account_key>
        </FromNamedCollectionWithAccount>
    </named_collections>
    </clickhouse>)CONFIG");

    loadNamedCollectionConfig(xml);

    StorageAzureConfigurationFriend conf;
    auto collection = NamedCollectionFactoryFriend::instance().get("FromNamedCollectionWithAccount");
    conf.fromNamedCollection(*collection, Context::getGlobalContextInstance());

    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Azure::Storage::StorageSharedKeyCredential>>(conf.getConnectionParams().auth_method));
}

TEST(StorageAzureConfiguration, FromNamedCollectionWithURL)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <FromNamedCollectionWithURL>
            <container>test_container</container>
            <blob_path>test_blob.csv</blob_path>
            <connection_string>https://azurite1:10000/devstoreaccount1?foo=bar</connection_string>
        </FromNamedCollectionWithURL>
    </named_collections>
    </clickhouse>)CONFIG");

    loadNamedCollectionConfig(xml);

    StorageAzureConfigurationFriend conf;
    auto collection = NamedCollectionFactoryFriend::instance().get("FromNamedCollectionWithURL");
    conf.fromNamedCollection(*collection, Context::getGlobalContextInstance());

    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Azure::Identity::ManagedIdentityCredential>>(conf.getConnectionParams().auth_method));
}

TEST(StorageAzureConfiguration, FromNamedCollectionWithExtraCredentialsAndAccount)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <FromNamedCollectionWithExtraCredentialsAndAccount>
            <container>test_container</container>
            <blob_path>test_blob.csv</blob_path>
            <account_name>test_account</account_name>
            <account_key>test_key</account_key>
            <client_id>test_client_id</client_id>
            <tenant_id>test_tenant_id</tenant_id>
        </FromNamedCollectionWithExtraCredentialsAndAccount>
    </named_collections>
    </clickhouse>)CONFIG");

    loadNamedCollectionConfig(xml);

    StorageAzureConfigurationFriend conf;
    auto collection = NamedCollectionFactoryFriend::instance().get("FromNamedCollectionWithExtraCredentialsAndAccount");

    ASSERT_THROW_ERROR_CODE(conf.fromNamedCollection(*collection, Context::getGlobalContextInstance()), Exception, ErrorCodes::BAD_ARGUMENTS, "Choose only one");
}

TEST(StorageAzureConfiguration, FromNamedCollectionWithPartialExtraCredentials)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <FromNamedCollectionWithPartialExtraCredentials>
            <container>test_container</container>
            <blob_path>test_blob.csv</blob_path>
            <account_name>test_account</account_name>
            <account_key>test_key</account_key>
            <client_id>test_client_id</client_id>
        </FromNamedCollectionWithPartialExtraCredentials>
    </named_collections>
    </clickhouse>)CONFIG");

    loadNamedCollectionConfig(xml);

    StorageAzureConfigurationFriend conf;
    auto collection = NamedCollectionFactoryFriend::instance().get("FromNamedCollectionWithPartialExtraCredentials");

    ASSERT_THROW_ERROR_CODE(conf.fromNamedCollection(*collection, Context::getGlobalContextInstance()), Exception, ErrorCodes::BAD_ARGUMENTS, "'tenant_id' is missing");
}

// Helper to get engine args from a query string
ASTs getEngineArgs(const std::string & query)
{
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, 0, 0, 0);
    return ast->as<ASTDescribeQuery>()->table_expression->as<ASTTableExpression>()->table_function->as<ASTFunction>()->arguments->children;
}

TEST(StorageAzureConfiguration, FromASTWithExtraCredentials)
{
    std::string query = "DESCRIBE TABLE azureBlobStorage('https://azurite1:10000/devstoreaccount1', 'test_container', 'test_blob.csv', extra_credentials(client_id='test_client_id', tenant_id='test_tenant_id'))";

    ASTs engine_args = getEngineArgs(query);
    StorageAzureConfigurationFriend conf;
    conf.fromAST(engine_args, Context::getGlobalContextInstance(), false);

    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Azure::Identity::WorkloadIdentityCredential>>(conf.getConnectionParams().auth_method));
}

TEST(StorageAzureConfiguration, FromASTWithAccount)
{
    std::string query = "DESCRIBE TABLE azureBlobStorage('https://azurite1:10000/devstoreaccount1', 'test_container', 'test_blob.csv', 'account_name', 'account_key')";

    ASTs engine_args = getEngineArgs(query);
    StorageAzureConfigurationFriend conf;
    conf.fromAST(engine_args, Context::getGlobalContextInstance(), false);

    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Azure::Storage::StorageSharedKeyCredential>>(conf.getConnectionParams().auth_method));
}

TEST(StorageAzureConfiguration, FromASTWithURL)
{
    std::string query = "DESCRIBE TABLE azureBlobStorage('https://azurite1:10000/devstoreaccount1?foo=bar', 'test_container', 'test_blob.csv')";

    ASTs engine_args = getEngineArgs(query);
    StorageAzureConfigurationFriend conf;
    conf.fromAST(engine_args, Context::getGlobalContextInstance(), false);

    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<Azure::Identity::ManagedIdentityCredential>>(conf.getConnectionParams().auth_method));
}

TEST(StorageAzureConfiguration, FromASTWithExtraCredentialsAndAccount)
{
    std::string query = "DESCRIBE TABLE azureBlobStorage('https://azurite1:10000/devstoreaccount1', 'test_container', 'test_blob.csv', 'account_name', 'account_key', extra_credentials(client_id='test_client_id', tenant_id='test_tenant_id'))";

    ASTs engine_args = getEngineArgs(query);
    StorageAzureConfigurationFriend conf;

    ASSERT_THROW_ERROR_CODE(conf.fromAST(engine_args, Context::getGlobalContextInstance(), false), Exception, ErrorCodes::BAD_ARGUMENTS, "Choose only one");
}

TEST(StorageAzureConfiguration, FromASTWithPartialExtraCredentials)
{
    std::string query = "DESCRIBE TABLE azureBlobStorage('https://azurite1:10000/devstoreaccount1', 'test_container', 'test_blob.csv', 'account_name', 'account_key', extra_credentials(tenant_id='test_tenant_id'))";

    ASTs engine_args = getEngineArgs(query);
    StorageAzureConfigurationFriend conf;

    ASSERT_THROW_ERROR_CODE(conf.fromAST(engine_args, Context::getGlobalContextInstance(), false), Exception, ErrorCodes::BAD_ARGUMENTS, "'client_id' is missing");
}

}

#endif
