#include "config.h"

#include <gtest/gtest.h>

#include <DataTypes/DataTypeString.h>

#if USE_DELTA_KERNEL_RS

#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/logger_useful.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SetSerialization.h>

#include <Storages/ObjectStorage/DataLakes/DeltaLake/ExpressionVisitor.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/StreamChannel.h>

#include "delta_kernel_ffi.hpp"

namespace DB { namespace ErrorCodes { extern const int NOT_IMPLEMENTED; extern const int ACCESS_DENIED; } }

class DeltaKernelTest : public testing::Test
{
public:
    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);

        if (const char * test_log_level = std::getenv("TEST_LOG_LEVEL")) // NOLINT(concurrency-mt-unsafe)
            Poco::Logger::root().setLevel(test_log_level);
        else
            Poco::Logger::root().setLevel("none");
    }

    void TearDown() override {}
};


TEST_F(DeltaKernelTest, ExpressionVisitor)
{
    auto * predicate = ffi::get_testing_kernel_predicate();
    SCOPE_EXIT(ffi::free_kernel_predicate(predicate));
    try
    {
        auto dag = DeltaLake::visitExpression(
            predicate,
            DB::NamesAndTypesList({DB::NameAndTypePair("col", std::make_shared<DB::DataTypeString>())}),
            DB::NamesAndTypesList({DB::NameAndTypePair("col", std::make_shared<DB::DataTypeString>())}));
    }
    catch (DB::Exception & e)
    {
        const std::string & message = e.message();
        if (e.code() == DB::ErrorCodes::NOT_IMPLEMENTED && message == "Method IN not implemented")
        {
            /// Implementation is not full at this moment, but
            /// there is a lot of staff before we get to IN method,
            /// so let's make sure everything before IN works.
            return;
        }
        LOG_ERROR(getLogger("Test"), "Exception: {}", message);
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(false);
}

#endif

#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaSchemaJSON.h>

/// varchar(n) and char(n) are valid Delta Lake column types emitted by Spark/Databricks.
/// They must map to String, ignoring the length constraint, since it is a SQL-layer annotation
/// only — the underlying Parquet encoding is identical to a plain string column.
TEST(DeltaSchemaJSON, GetSimpleTypeByNameVarchar)
{
    auto type = DB::getDeltaLakeSimpleTypeByName("varchar(256)");
    ASSERT_NE(type, nullptr);
    ASSERT_EQ(type->getTypeId(), DB::TypeIndex::String);
}

TEST(DeltaSchemaJSON, GetSimpleTypeByNameChar)
{
    auto type = DB::getDeltaLakeSimpleTypeByName("char(1)");
    ASSERT_NE(type, nullptr);
    ASSERT_EQ(type->getTypeId(), DB::TypeIndex::String);
}

#if USE_DELTA_KERNEL_RS && USE_AZURE_BLOB_STORAGE

#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>
#include <azure/identity/workload_identity_credential.hpp>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <cstdlib>
#include <optional>
#include <sstream>

namespace
{
std::optional<std::string> findBuilderOption(
    const std::vector<std::pair<std::string, std::string>> & options, const std::string & name)
{
    for (const auto & [k, v] : options)
        if (k == name)
            return v;
    return std::nullopt;
}

}

/// Empty connection string
TEST(DeltaLakeAzureKernelHelper, VendedSasTokenSetsAccountName)
{
    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://testaccount.blob.core.windows.net";
    params.endpoint.container_name = "testcontainer";
    params.endpoint.sas_auth = "sv=2021-06-08&sig=abcDEF123";
    /// auth_method intentionally left default: the empty ConnectionString alternative that
    /// the vended-credentials / Unity catalog path produces.
    ASSERT_EQ(params.auth_method.index(), 0u);

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto account = findBuilderOption(options, "azure_storage_account_name");
    ASSERT_TRUE(account.has_value());
    ASSERT_EQ(*account, "testaccount");

    const auto sas = findBuilderOption(options, "azure_storage_sas_key");
    ASSERT_TRUE(sas.has_value());
    ASSERT_EQ(*sas, "sv=2021-06-08&sig=abcDEF123");

    const auto container = findBuilderOption(options, "azure_container_name");
    ASSERT_TRUE(container.has_value());
    ASSERT_EQ(*container, "testcontainer");

    /// The endpoint URL must be passed through as is, without connection string parsing.
    const auto endpoint_url = findBuilderOption(options, "azure_endpoint");
    ASSERT_TRUE(endpoint_url.has_value());
    ASSERT_EQ(*endpoint_url, "https://testaccount.blob.core.windows.net");
    ASSERT_FALSE(findBuilderOption(options, "azure_allow_http").has_value());
}

/// An explicit https endpoint (a sovereign cloud, a private link) must be passed to the
/// kernel, which would otherwise derive the public host from the account name. Plain HTTP
/// must stay disallowed.
TEST(DeltaLakeAzureKernelHelper, ExplicitHttpsEndpointIsPassedToKernel)
{
    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://testaccount.blob.core.chinacloudapi.cn";
    params.endpoint.container_name = "testcontainer";
    params.endpoint.account_name = "testaccount";
    params.auth_method = std::make_shared<Azure::Storage::StorageSharedKeyCredential>("testaccount", "dGVzdGtleQ==");

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto endpoint_url = findBuilderOption(options, "azure_endpoint");
    ASSERT_TRUE(endpoint_url.has_value());
    ASSERT_EQ(*endpoint_url, "https://testaccount.blob.core.chinacloudapi.cn");
    ASSERT_FALSE(findBuilderOption(options, "azure_allow_http").has_value());
}

/// A plain-HTTP endpoint (the Azurite emulator) must additionally allow plain HTTP,
/// since the object-store builder is https-only by default.
TEST(DeltaLakeAzureKernelHelper, ExplicitHttpEndpointAllowsPlainHttp)
{
    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "http://127.0.0.1:10000/testaccount";
    params.endpoint.container_name = "testcontainer";
    params.endpoint.account_name = "testaccount";
    params.auth_method = std::make_shared<Azure::Storage::StorageSharedKeyCredential>("testaccount", "dGVzdGtleQ==");

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto endpoint_url = findBuilderOption(options, "azure_endpoint");
    ASSERT_TRUE(endpoint_url.has_value());
    ASSERT_EQ(*endpoint_url, "http://127.0.0.1:10000/testaccount");

    const auto allow_http = findBuilderOption(options, "azure_allow_http");
    ASSERT_TRUE(allow_http.has_value());
    ASSERT_EQ(*allow_http, "true");
}

/// An endpoint-style disk configuration (`<endpoint>http://host:port/account/container/prefix</endpoint>`)
/// keeps only the scheme and host in `storage_account_url` and carries the account name as a
/// separate path segment (`add_account_name_to_url` is set). The kernel must receive the endpoint
/// with the account segment appended, the same URL the SDK client itself uses.
TEST(DeltaLakeAzureKernelHelper, EndpointStyleConfigAppendsAccountName)
{
    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "http://127.0.0.1:10000";
    params.endpoint.container_name = "testcontainer";
    params.endpoint.account_name = "devstoreaccount1";
    params.endpoint.prefix = "sub/path";
    params.endpoint.add_account_name_to_url = true;
    params.auth_method = std::make_shared<Azure::Storage::StorageSharedKeyCredential>("devstoreaccount1", "dGVzdGtleQ==");

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto endpoint_url = findBuilderOption(options, "azure_endpoint");
    ASSERT_TRUE(endpoint_url.has_value());
    ASSERT_EQ(*endpoint_url, "http://127.0.0.1:10000/devstoreaccount1");

    const auto allow_http = findBuilderOption(options, "azure_allow_http");
    ASSERT_TRUE(allow_http.has_value());
    ASSERT_EQ(*allow_http, "true");
}

/// A real connection string (non-empty ConnectionString alternative) must still be parsed
/// into its components, including the account name.
TEST(DeltaLakeAzureKernelHelper, ConnectionStringSetsAccountName)
{
    DB::AzureBlobStorage::ConnectionParams params;
    const std::string connection_string =
        "DefaultEndpointsProtocol=https;AccountName=testaccount;"
        "AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net";
    params.endpoint.storage_account_url = connection_string;
    params.endpoint.container_name = "testcontainer";
    params.auth_method = DB::AzureBlobStorage::ConnectionString{connection_string};
    ASSERT_EQ(params.auth_method.index(), 0u);

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto account = findBuilderOption(options, "azure_storage_account_name");
    ASSERT_TRUE(account.has_value());
    ASSERT_EQ(*account, "testaccount");

    const auto key = findBuilderOption(options, "azure_storage_account_key");
    ASSERT_TRUE(key.has_value());
    ASSERT_EQ(*key, "dGVzdGtleQ==");
    ASSERT_FALSE(findBuilderOption(options, "azure_skip_signature").has_value());
}

/// A connection string without a key or a SAS token must not let the kernel fall back to the managed identity.
TEST(DeltaLakeAzureKernelHelper, KeylessConnectionStringSkipsSignature)
{
    DB::AzureBlobStorage::ConnectionParams params;
    const std::string connection_string = "AccountName=testaccount;BlobEndpoint=https://endpoint.example;";
    params.endpoint.storage_account_url = connection_string;
    params.endpoint.container_name = "testcontainer";
    params.auth_method = DB::AzureBlobStorage::ConnectionString{connection_string};
    params.forbid_implicit_credentials = true;

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    ASSERT_FALSE(findBuilderOption(options, "azure_storage_account_key").has_value());
    const auto skip_signature = findBuilderOption(options, "azure_skip_signature");
    ASSERT_TRUE(skip_signature.has_value());
    ASSERT_EQ(*skip_signature, "true");
}

/// The same holds for an empty connection string without a SAS token.
TEST(DeltaLakeAzureKernelHelper, EmptyConnectionStringWithoutSasSkipsSignature)
{
    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://testaccount.blob.core.windows.net";
    params.endpoint.container_name = "testcontainer";
    params.forbid_implicit_credentials = true;
    ASSERT_EQ(params.auth_method.index(), 0u);

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto skip_signature = findBuilderOption(options, "azure_skip_signature");
    ASSERT_TRUE(skip_signature.has_value());
    ASSERT_EQ(*skip_signature, "true");
}

/// A keyless connection string is unsigned even with the opt-in: the Azure SDK treats it as anonymous.
TEST(DeltaLakeAzureKernelHelper, KeylessConnectionStringWithOptInIsUnsigned)
{
    DB::AzureBlobStorage::ConnectionParams params;
    const std::string connection_string = "AccountName=testaccount;BlobEndpoint=https://endpoint.example;";
    params.endpoint.storage_account_url = connection_string;
    params.endpoint.container_name = "testcontainer";
    params.auth_method = DB::AzureBlobStorage::ConnectionString{connection_string};
    params.forbid_implicit_credentials = false;

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto skip_signature = findBuilderOption(options, "azure_skip_signature");
    ASSERT_TRUE(skip_signature.has_value());
    ASSERT_EQ(*skip_signature, "true");
}

namespace
{
/// Sets an environment variable for the lifetime of the object and restores the previous value.
class ScopedEnv
{
public:
    ScopedEnv(const char * name_, const char * value) : name(name_)
    {
        if (const char * old = std::getenv(name)) // NOLINT(concurrency-mt-unsafe)
            old_value = old;
        if (value)
            setenv(name, value, 1); // NOLINT(concurrency-mt-unsafe)
        else
            unsetenv(name); // NOLINT(concurrency-mt-unsafe)
    }

    ~ScopedEnv()
    {
        if (old_value)
            setenv(name, old_value->c_str(), 1); // NOLINT(concurrency-mt-unsafe)
        else
            unsetenv(name); // NOLINT(concurrency-mt-unsafe)
    }

private:
    const char * name;
    std::optional<std::string> old_value;
};

DB::AzureBlobStorage::ConnectionParams makeWorkloadIdentityParams()
{
    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://testaccount.blob.core.windows.net";
    params.endpoint.container_name = "testcontainer";
    params.auth_method = std::make_shared<Azure::Identity::WorkloadIdentityCredential>(Azure::Identity::WorkloadIdentityCredentialOptions{});
    params.forbid_implicit_credentials = false;
    return params;
}
}

/// `extra_credentials(client_id = ..., tenant_id = ...)` and `use_workload_identity` produce a
/// `WorkloadIdentityCredential`; the kernel must authenticate with the same workload identity.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentityExplicitClientAndTenant)
{
    ScopedEnv client_env("AZURE_CLIENT_ID", "env-client");
    ScopedEnv tenant_env("AZURE_TENANT_ID", "env-tenant");
    ScopedEnv token_env("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token");
    ScopedEnv authority_env("AZURE_AUTHORITY_HOST", "https://login.microsoftonline.us/");

    auto params = makeWorkloadIdentityParams();
    params.workload_identity_client_id = "explicit-client";
    params.workload_identity_tenant_id = "explicit-tenant";

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    ASSERT_EQ(findBuilderOption(options, "azure_storage_account_name"), "testaccount");
    ASSERT_EQ(findBuilderOption(options, "azure_client_id"), "explicit-client");
    ASSERT_EQ(findBuilderOption(options, "azure_tenant_id"), "explicit-tenant");
    ASSERT_EQ(findBuilderOption(options, "azure_federated_token_file"), "/var/run/secrets/azure/tokens/azure-identity-token");
    ASSERT_EQ(findBuilderOption(options, "azure_authority_host"), "https://login.microsoftonline.us");
    ASSERT_FALSE(findBuilderOption(options, "azure_storage_account_key").has_value());
    ASSERT_FALSE(findBuilderOption(options, "azure_skip_signature").has_value());
}

/// Without `extra_credentials`, the identity comes from the environment, as for the Azure SDK.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentityFromEnvironment)
{
    ScopedEnv client_env("AZURE_CLIENT_ID", "env-client");
    ScopedEnv tenant_env("AZURE_TENANT_ID", "env-tenant");
    ScopedEnv token_env("AZURE_FEDERATED_TOKEN_FILE", "/token");
    ScopedEnv authority_env("AZURE_AUTHORITY_HOST", nullptr);

    const auto options = DeltaLake::getAzureBuilderOptions(makeWorkloadIdentityParams());

    ASSERT_EQ(findBuilderOption(options, "azure_client_id"), "env-client");
    ASSERT_EQ(findBuilderOption(options, "azure_tenant_id"), "env-tenant");
    ASSERT_EQ(findBuilderOption(options, "azure_federated_token_file"), "/token");
    ASSERT_EQ(findBuilderOption(options, "azure_authority_host"), "https://login.microsoftonline.com");
}

/// The object store would silently use the managed identity of the machine without the federated token file.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentityWithoutTokenFileThrows)
{
    ScopedEnv client_env("AZURE_CLIENT_ID", "env-client");
    ScopedEnv tenant_env("AZURE_TENANT_ID", "env-tenant");
    ScopedEnv token_env("AZURE_FEDERATED_TOKEN_FILE", nullptr);

    ASSERT_THROW(DeltaLake::getAzureBuilderOptions(makeWorkloadIdentityParams()), DB::Exception);
}

/// With the anonymous fallback for server credentials, the request is unsigned and no identity is passed.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentityAnonymousFallbackSkipsSignature)
{
    auto params = makeWorkloadIdentityParams();
    params.forbid_implicit_credentials = true;
    params.anonymous_fallback_for_server_credentials = true;

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    ASSERT_EQ(findBuilderOption(options, "azure_skip_signature"), "true");
    ASSERT_FALSE(findBuilderOption(options, "azure_client_id").has_value());
    ASSERT_FALSE(findBuilderOption(options, "azure_federated_token_file").has_value());
}

/// Non-default HTTPS hosts (sovereign clouds, custom domains) must be forwarded as
/// azure_endpoint, or the builder falls back to the default public host.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentitySetsEndpointForNonDefaultHost)
{
    ScopedEnv env_tenant("AZURE_TENANT_ID", "11111111-1111-1111-1111-111111111111");
    ScopedEnv env_client("AZURE_CLIENT_ID", "22222222-2222-2222-2222-222222222222");
    ScopedEnv env_token_file("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token");

    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://testaccount.blob.core.usgovcloudapi.net";
    params.endpoint.container_name = "testcontainer";
    params.auth_method = std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
    params.forbid_implicit_credentials = false;
    ASSERT_EQ(params.auth_method.index(), 3u);

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto azure_endpoint = findBuilderOption(options, "azure_endpoint");
    ASSERT_TRUE(azure_endpoint.has_value());
    ASSERT_EQ(*azure_endpoint, "https://testaccount.blob.core.usgovcloudapi.net");

    ASSERT_FALSE(findBuilderOption(options, "azure_allow_http").has_value());
}

/// Explicit IDs must survive the stored engine args (detach/attach, restart) and still
/// win over conflicting environment IDs when the table is parsed again.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentityExplicitIdsSurviveReattach)
{
    ScopedEnv env_tenant("AZURE_TENANT_ID", "88888888-8888-8888-8888-888888888888");
    ScopedEnv env_client("AZURE_CLIENT_ID", "99999999-9999-9999-9999-999999999999");
    ScopedEnv env_token_file("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token");

    const std::string query = "DeltaLakeAzure('https://testaccount.blob.core.windows.net', 'testcontainer', 'table/', "
                              "extra_credentials(client_id='22222222-2222-2222-2222-222222222222', tenant_id='11111111-1111-1111-1111-111111111111'))";
    DB::ParserExpression parser;
    DB::ASTPtr ast = DB::parseQuery(parser, query, 1000, 1000, 1000000);
    DB::ASTs engine_args = ast->as<DB::ASTFunction>()->arguments->children;

    DB::AzureStorageParsedArguments created;
    created.fromAST(engine_args, getContext().context, /*with_structure=*/ false);

    /// Parse the same args again, as on attach.
    DB::AzureStorageParsedArguments attached;
    attached.fromAST(engine_args, getContext().context, /*with_structure=*/ false);
    attached.connection_params.forbid_implicit_credentials = false;
    ASSERT_EQ(attached.connection_params.auth_method.index(), 3u);

    const auto options = DeltaLake::getAzureBuilderOptions(attached.connection_params);

    const auto tenant = findBuilderOption(options, "azure_tenant_id");
    ASSERT_TRUE(tenant.has_value());
    ASSERT_EQ(*tenant, "11111111-1111-1111-1111-111111111111");

    const auto client = findBuilderOption(options, "azure_client_id");
    ASSERT_TRUE(client.has_value());
    ASSERT_EQ(*client, "22222222-2222-2222-2222-222222222222");
}

/// Private-link hosts end with .blob.core.windows.net but are not the URL the builder
/// derives from the account name, so azure_endpoint must still be forwarded.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentitySetsEndpointForPrivateLinkHost)
{
    ScopedEnv env_tenant("AZURE_TENANT_ID", "11111111-1111-1111-1111-111111111111");
    ScopedEnv env_client("AZURE_CLIENT_ID", "22222222-2222-2222-2222-222222222222");
    ScopedEnv env_token_file("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token");

    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://testaccount.privatelink.blob.core.windows.net";
    params.endpoint.container_name = "testcontainer";
    params.auth_method = std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
    params.forbid_implicit_credentials = false;
    ASSERT_EQ(params.auth_method.index(), 3u);

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto azure_endpoint = findBuilderOption(options, "azure_endpoint");
    ASSERT_TRUE(azure_endpoint.has_value());
    ASSERT_EQ(*azure_endpoint, "https://testaccount.privatelink.blob.core.windows.net");
}

#if USE_PARQUET

namespace
{

/// A table storage that uses the server's identity, with the shared restriction flag left `false`, as a concurrent
/// opted-in session leaves it.
std::shared_ptr<DB::AzureObjectStorage> makeServerIdentityObjectStorage(bool follows_session_restriction)
{
    auto params = makeWorkloadIdentityParams();
    params.follows_session_restriction = follows_session_restriction;
    params.client_options.Retry.MaxRetries = 0;

    auto container_client = std::make_unique<DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient(
            params.endpoint.storage_account_url + "/" + params.endpoint.container_name, params.client_options),
        /* blob_prefix */ "");

    return std::make_shared<DB::AzureObjectStorage>(
        "azure",
        std::move(container_client),
        std::make_unique<DB::AzureBlobStorage::RequestSettings>(),
        params,
        /* object_namespace */ params.endpoint.container_name,
        /* description */ "azure",
        /* common_key_prefix */ "");
}

DB::ContextMutablePtr makeSessionContext(bool allow_server_credentials)
{
    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("s3_allow_server_credentials_in_user_queries", DB::Field(allow_server_credentials));
    return context;
}

template <typename F>
void expectAccessDenied(F && f)
{
    try
    {
        f();
        FAIL() << "Expected ACCESS_DENIED";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::ACCESS_DENIED) << e.message();
    }
}

/// The check passed: the call reached the kernel, which fails without a federated token file.
template <typename F>
void expectNotAccessDenied(F && f)
{
    try
    {
        f();
    }
    catch (const DB::Exception & e)
    {
        ASSERT_NE(e.code(), DB::ErrorCodes::ACCESS_DENIED) << e.message();
    }
}

}

/// A concurrent opted-in session resets the shared `forbid_implicit_credentials` to `false`, so each entrypoint
/// must refuse a restricted session by itself, before the snapshot cache or the kernel is used.
TEST(DeltaLakeAzureKernelHelper, EntrypointsRefuseRestrictedSessionWhateverTheSharedFlag)
{
    ScopedEnv token_env("AZURE_FEDERATED_TOKEN_FILE", nullptr);

    /// The restriction applies to the server only.
    const auto previous_application_type = getContext().context->getApplicationType();
    getContext().context->setApplicationType(DB::Context::ApplicationType::SERVER);
    SCOPE_EXIT(getContext().context->setApplicationType(previous_application_type));

    /// `totalRows` and `totalBytes` return no value on any error, so tell the refusal apart by what they log.
    std::ostringstream log_output; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto stream_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(log_output));
    auto metadata_log = getLogger("DeltaLakeMetadata");
    Poco::AutoPtr<Poco::Channel> previous_channel(metadata_log->getChannel(), /* shared */ true);
    const int previous_level = metadata_log->getLevel();
    metadata_log->setChannel(stream_channel.get());
    metadata_log->setLevel("error");
    SCOPE_EXIT({
        metadata_log->setChannel(previous_channel.get());
        metadata_log->setLevel(previous_level);
    });

    auto make_table = [](bool follows_session_restriction)
    {
        auto object_storage = makeServerIdentityObjectStorage(follows_session_restriction);
        auto configuration = std::make_shared<DB::StorageAzureDeltaLakeConfiguration>(std::make_shared<DB::DataLakeStorageSettings>());
        configuration->setRawPath(DB::StorageObjectStorageConfiguration::Path("table/"));
        configuration->format = "Parquet";
        auto metadata = std::make_shared<DB::DeltaLakeMetadataDeltaKernel>(object_storage, configuration);
        return std::make_tuple(object_storage, configuration, metadata);
    };

    const auto restricted = makeSessionContext(/* allow_server_credentials */ false);
    const auto allowed = makeSessionContext(/* allow_server_credentials */ true);
    const DB::StorageID table_id("db", "t");

    {
        auto [object_storage, configuration, metadata] = make_table(/* follows_session_restriction */ true);
        ASSERT_FALSE(object_storage->getAzureBlobStorageConnectionParams()->forbid_implicit_credentials);

        expectAccessDenied([&] { metadata->update(restricted); });
        expectAccessDenied([&] { metadata->getTableSchema(restricted); });
        expectAccessDenied([&] { metadata->getTableStateSnapshot(restricted); });
        expectAccessDenied([&] { metadata->buildStorageMetadataFromState(DeltaLake::TableStateSnapshot{}, restricted); });
        expectAccessDenied([&] { metadata->iterate(nullptr, {}, 1000, nullptr, restricted); });
        expectAccessDenied([&] { metadata->prepareReadingFromFormat({}, nullptr, restricted, true, true); });
        expectAccessDenied([&] { metadata->getTableChanges({0, std::nullopt}, DB::Block{}, std::nullopt, restricted); });
        expectAccessDenied([&]
        {
            metadata->write(
                std::make_shared<const DB::Block>(), table_id, object_storage, configuration, std::nullopt, restricted, nullptr);
        });
        expectAccessDenied([&]
        {
            DB::DeltaLakeMetadataDeltaKernel::createInitial(
                object_storage, configuration, restricted, std::nullopt, nullptr, nullptr, false, nullptr, table_id);
        });

        log_output.str("");
        ASSERT_FALSE(metadata->totalRows(restricted, std::nullopt).has_value());
        ASSERT_NE(log_output.str().find("server's own identity"), std::string::npos) << log_output.str();
        log_output.str("");
        ASSERT_FALSE(metadata->totalBytes(restricted).has_value());
        ASSERT_NE(log_output.str().find("server's own identity"), std::string::npos) << log_output.str();

        /// An opted-in session passes the check.
        expectNotAccessDenied([&] { metadata->update(allowed); });
        log_output.str("");
        ASSERT_FALSE(metadata->totalRows(allowed, std::nullopt).has_value());
        ASSERT_EQ(log_output.str().find("server's own identity"), std::string::npos) << log_output.str();
    }

    {
        /// A disk keeps the server's identity for every session.
        auto [object_storage, configuration, metadata] = make_table(/* follows_session_restriction */ false);
        expectNotAccessDenied([&] { metadata->update(restricted); });
        log_output.str("");
        ASSERT_FALSE(metadata->totalRows(restricted, std::nullopt).has_value());
        ASSERT_EQ(log_output.str().find("server's own identity"), std::string::npos) << log_output.str();
    }
}

#endif

#endif
