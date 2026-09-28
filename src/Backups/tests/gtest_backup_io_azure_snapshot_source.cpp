#include <gtest/gtest.h>
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Backups/BackupIO_AzureBlobStorage.h>
#include <Common/Exception.h>

#include <azure/storage/common/storage_credential.hpp>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

AzureBlobStorage::ConnectionParams backupConnectionParams(AzureBlobStorage::AuthMethod auth_method)
{
    AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "https://backups.blob.core.windows.net";
    params.endpoint.container_name = "backups";
    params.endpoint.prefix = "2026/backup1";
    params.auth_method = std::move(auth_method);
    return params;
}

AzureBlobStorage::AuthMethod sharedKey()
{
    return std::make_shared<Azure::Storage::StorageSharedKeyCredential>("account", "key");
}

}

TEST(BackupIOAzureSnapshotSource, SplitsNamespaceIntoContainerAndPrefix)
{
    const auto backup = backupConnectionParams(sharedKey());
    const auto source = makeSnapshotSourceConnectionParams(backup, "https://source.blob.core.windows.net", "data/mergetree/");

    EXPECT_EQ(source.endpoint.storage_account_url, "https://source.blob.core.windows.net");
    EXPECT_EQ(source.endpoint.container_name, "data");
    EXPECT_EQ(source.endpoint.prefix, "mergetree/");
    EXPECT_EQ(source.endpoint.getContainerEndpoint(), "https://source.blob.core.windows.net/data");
    EXPECT_TRUE(source.endpoint.container_already_exists.value_or(false));

    /// The backup's own connection params are left untouched.
    EXPECT_EQ(backup.endpoint.storage_account_url, "https://backups.blob.core.windows.net");
    EXPECT_EQ(backup.endpoint.container_name, "backups");
    EXPECT_EQ(backup.endpoint.prefix, "2026/backup1");
    EXPECT_FALSE(backup.endpoint.container_already_exists.has_value());
}

TEST(BackupIOAzureSnapshotSource, NamespaceWithoutPrefix)
{
    const auto source
        = makeSnapshotSourceConnectionParams(backupConnectionParams(sharedKey()), "https://source.blob.core.windows.net", "data");

    EXPECT_EQ(source.endpoint.container_name, "data");
    EXPECT_EQ(source.endpoint.prefix, "");
    EXPECT_EQ(source.endpoint.getContainerEndpoint(), "https://source.blob.core.windows.net/data");
}

namespace
{

AzureBlobStorage::ConnectionParams connectionStringBackup()
{
    const String connection_string = "DefaultEndpointsProtocol=https;AccountName=backups;AccountKey=a2V5;EndpointSuffix=core.windows.net";
    auto backup = backupConnectionParams(AzureBlobStorage::ConnectionString{connection_string});
    backup.endpoint.storage_account_url = connection_string;
    return backup;
}

}

TEST(BackupIOAzureSnapshotSource, ConnectionStringOfTheSnapshotAccountIsKept)
{
    const auto backup = connectionStringBackup();

    /// The recorded endpoint is the backup's own account (a trailing slash must not matter).
    const auto source = makeSnapshotSourceConnectionParams(backup, "https://backups.blob.core.windows.net/", "data/mergetree/");

    EXPECT_EQ(source.endpoint.storage_account_url, backup.endpoint.storage_account_url);
    EXPECT_EQ(source.endpoint.container_name, "data");
    EXPECT_EQ(source.endpoint.prefix, "mergetree/");
}

TEST(BackupIOAzureSnapshotSource, ConnectionStringOfAnotherAccountIsRejected)
{
    const auto backup = connectionStringBackup();

    try
    {
        makeSnapshotSourceConnectionParams(backup, "https://source.blob.core.windows.net", "data/mergetree/");
        FAIL() << "expected BAD_ARGUMENTS";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("cannot access another storage account"), String::npos);
    }
}

#endif
