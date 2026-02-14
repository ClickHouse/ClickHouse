#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Databases/DataLake/DatabaseDataLakeStorageType.h>
#include <Databases/DataLake/StaticStorageCredentials.h>
#include <Databases/DataLake/StorageCredentials.h>
#include <Common/SettingsChanges.h>
#include <gtest/gtest.h>

namespace DataLake::Test
{

namespace
{
DB::DatabaseDataLakeSettings makeSettingsWithChanges(const DB::SettingsChanges & changes)
{
    DB::DatabaseDataLakeSettings settings;
    settings.applyChanges(changes);
    return settings;
}
}

TEST(StaticStorageCredentialsTest, ReturnsS3CredentialsFromAwsSettings)
{
    DB::SettingsChanges changes;
    changes.emplace_back("aws_access_key_id", "gcs_hmac_access_key");
    changes.emplace_back("aws_secret_access_key", "gcs_hmac_secret_key");
    auto settings = makeSettingsWithChanges(changes);

    auto credentials = tryGetStaticStorageCredentials(DB::DatabaseDataLakeStorageType::S3, settings);
    ASSERT_NE(credentials, nullptr);

    auto s3_credentials = std::dynamic_pointer_cast<S3Credentials>(credentials);
    ASSERT_NE(s3_credentials, nullptr);
    EXPECT_EQ(s3_credentials->getAccessKeyId(), "gcs_hmac_access_key");
    EXPECT_EQ(s3_credentials->getSecretAccessKey(), "gcs_hmac_secret_key");
    EXPECT_EQ(s3_credentials->getSessionToken(), "");
}

TEST(StaticStorageCredentialsTest, ReturnsS3CredentialsFromStoragePrefixedSettings)
{
    DB::SettingsChanges changes;
    changes.emplace_back("storage_aws_access_key_id", "storage_access");
    changes.emplace_back("storage_aws_secret_access_key", "storage_secret");
    auto settings = makeSettingsWithChanges(changes);

    auto credentials = tryGetStaticStorageCredentials(DB::DatabaseDataLakeStorageType::S3, settings);
    ASSERT_NE(credentials, nullptr);

    auto s3_credentials = std::dynamic_pointer_cast<S3Credentials>(credentials);
    ASSERT_NE(s3_credentials, nullptr);
    EXPECT_EQ(s3_credentials->getAccessKeyId(), "storage_access");
    EXPECT_EQ(s3_credentials->getSecretAccessKey(), "storage_secret");
}

TEST(StaticStorageCredentialsTest, PrefersNonPrefixedSettingsWhenBothSet)
{
    DB::SettingsChanges changes;
    changes.emplace_back("aws_access_key_id", "preferred_access");
    changes.emplace_back("aws_secret_access_key", "preferred_secret");
    changes.emplace_back("storage_aws_access_key_id", "fallback_access");
    changes.emplace_back("storage_aws_secret_access_key", "fallback_secret");
    auto settings = makeSettingsWithChanges(changes);

    auto credentials = tryGetStaticStorageCredentials(DB::DatabaseDataLakeStorageType::S3, settings);
    ASSERT_NE(credentials, nullptr);

    auto s3_credentials = std::dynamic_pointer_cast<S3Credentials>(credentials);
    ASSERT_NE(s3_credentials, nullptr);
    EXPECT_EQ(s3_credentials->getAccessKeyId(), "preferred_access");
    EXPECT_EQ(s3_credentials->getSecretAccessKey(), "preferred_secret");
}

TEST(StaticStorageCredentialsTest, ReturnsNullptrWhenSecretIsMissing)
{
    DB::SettingsChanges changes;
    changes.emplace_back("aws_access_key_id", "only_access");
    auto settings = makeSettingsWithChanges(changes);

    auto credentials = tryGetStaticStorageCredentials(DB::DatabaseDataLakeStorageType::S3, settings);
    EXPECT_EQ(credentials, nullptr);
}

TEST(StaticStorageCredentialsTest, ReturnsNullptrForNonS3Storage)
{
    DB::SettingsChanges changes;
    changes.emplace_back("aws_access_key_id", "access");
    changes.emplace_back("aws_secret_access_key", "secret");
    auto settings = makeSettingsWithChanges(changes);

    auto credentials = tryGetStaticStorageCredentials(DB::DatabaseDataLakeStorageType::Azure, settings);
    EXPECT_EQ(credentials, nullptr);
}

}
