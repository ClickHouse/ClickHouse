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

namespace DB { namespace ErrorCodes { extern const int NOT_IMPLEMENTED; } }

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
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>

#include <azure/identity/workload_identity_credential.hpp>

#include <cstdlib>
#include <optional>

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

#endif
