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

#if USE_PARQUET

#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Core/Field.h>

/// Regression test for segfault
TEST(DeltaLakeMetadata, GetFieldValueNullableDateTime64)
{
    auto nullable_datetime64_type = std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeDateTime64>(6, "UTC"));
    ASSERT_NO_THROW(DB::DeltaLakeMetadata::getFieldValue("2024-01-15 10:30:45.123456", nullable_datetime64_type));
}

/// varchar(n) and char(n) are valid Delta Lake column types emitted by Spark/Databricks.
/// They must map to String, ignoring the length constraint, since it is a SQL-layer annotation
/// only — the underlying Parquet encoding is identical to a plain string column.
TEST(DeltaLakeMetadata, GetSimpleTypeByNameVarchar)
{
    auto type = DB::DeltaLakeMetadata::getSimpleTypeByName("varchar(256)");
    ASSERT_NE(type, nullptr);
    ASSERT_EQ(type->getTypeId(), DB::TypeIndex::String);
}

TEST(DeltaLakeMetadata, GetSimpleTypeByNameChar)
{
    auto type = DB::DeltaLakeMetadata::getSimpleTypeByName("char(1)");
    ASSERT_NE(type, nullptr);
    ASSERT_EQ(type->getTypeId(), DB::TypeIndex::String);
}

#endif

#if USE_DELTA_KERNEL_RS && USE_AZURE_BLOB_STORAGE

#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
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
}

/// Workload identity credentials must be forwarded as builder options because
/// object_store does not read the environment on its own.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentityForwardsEnvironment)
{
    setenv("AZURE_TENANT_ID", "11111111-1111-1111-1111-111111111111", 1); // NOLINT(concurrency-mt-unsafe)
    setenv("AZURE_CLIENT_ID", "22222222-2222-2222-2222-222222222222", 1); // NOLINT(concurrency-mt-unsafe)
    setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token", 1); // NOLINT(concurrency-mt-unsafe)
    unsetenv("AZURE_AUTHORITY_HOST"); // NOLINT(concurrency-mt-unsafe)
    SCOPE_EXIT({
        unsetenv("AZURE_TENANT_ID"); // NOLINT(concurrency-mt-unsafe)
        unsetenv("AZURE_CLIENT_ID"); // NOLINT(concurrency-mt-unsafe)
        unsetenv("AZURE_FEDERATED_TOKEN_FILE"); // NOLINT(concurrency-mt-unsafe)
    });

    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://testaccount.blob.core.windows.net";
    params.endpoint.container_name = "testcontainer";
    params.auth_method = std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
    ASSERT_EQ(params.auth_method.index(), 3u);

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    const auto account = findBuilderOption(options, "azure_storage_account_name");
    ASSERT_TRUE(account.has_value());
    ASSERT_EQ(*account, "testaccount");

    const auto tenant = findBuilderOption(options, "azure_tenant_id");
    ASSERT_TRUE(tenant.has_value());
    ASSERT_EQ(*tenant, "11111111-1111-1111-1111-111111111111");

    const auto client = findBuilderOption(options, "azure_client_id");
    ASSERT_TRUE(client.has_value());
    ASSERT_EQ(*client, "22222222-2222-2222-2222-222222222222");

    const auto token_file = findBuilderOption(options, "azure_federated_token_file");
    ASSERT_TRUE(token_file.has_value());
    ASSERT_EQ(*token_file, "/var/run/secrets/azure/tokens/azure-identity-token");

    ASSERT_FALSE(findBuilderOption(options, "azure_authority_host").has_value());
}

/// Explicit client_id / tenant_id (extra_credentials, named collections) must be
/// forwarded even when the environment variables are absent, and win over them.
/// Built through getAzureConnectionParams so the test covers the persistence of
/// the IDs on ConnectionParams, not just the builder plumbing.
TEST(DeltaLakeAzureKernelHelper, WorkloadIdentityForwardsExplicitIds)
{
    unsetenv("AZURE_TENANT_ID"); // NOLINT(concurrency-mt-unsafe)
    setenv("AZURE_CLIENT_ID", "99999999-9999-9999-9999-999999999999", 1); // NOLINT(concurrency-mt-unsafe)
    setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token", 1); // NOLINT(concurrency-mt-unsafe)
    SCOPE_EXIT({
        unsetenv("AZURE_CLIENT_ID"); // NOLINT(concurrency-mt-unsafe)
        unsetenv("AZURE_FEDERATED_TOKEN_FILE"); // NOLINT(concurrency-mt-unsafe)
    });

    const auto params = DB::getAzureConnectionParams(
        "https://testaccount.blob.core.windows.net",
        "testcontainer",
        /*account_name*/ std::nullopt,
        /*account_key*/ std::nullopt,
        "22222222-2222-2222-2222-222222222222",
        "11111111-1111-1111-1111-111111111111",
        getContext().context);
    ASSERT_EQ(params.auth_method.index(), 3u);

    const auto options = DeltaLake::getAzureBuilderOptions(params);

    /// Explicit tenant id, despite no AZURE_TENANT_ID in the environment.
    const auto tenant = findBuilderOption(options, "azure_tenant_id");
    ASSERT_TRUE(tenant.has_value());
    ASSERT_EQ(*tenant, "11111111-1111-1111-1111-111111111111");

    /// Explicit client id wins over the conflicting AZURE_CLIENT_ID.
    const auto client = findBuilderOption(options, "azure_client_id");
    ASSERT_TRUE(client.has_value());
    ASSERT_EQ(*client, "22222222-2222-2222-2222-222222222222");

    const auto token_file = findBuilderOption(options, "azure_federated_token_file");
    ASSERT_TRUE(token_file.has_value());
    ASSERT_EQ(*token_file, "/var/run/secrets/azure/tokens/azure-identity-token");
}

#endif
