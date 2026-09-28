#include "config.h"

#if USE_AWS_S3
#include <gtest/gtest.h>
#include <memory>
#include <unordered_set>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/ObjectStorage/Utils.h>

TEST(StorageS3MRAP, ObjectIdentityPreservesLiteralKeys)
{
    DB::StorageS3Configuration configuration;
    configuration.url = DB::S3::URI::fromMRAPArn(
        "arn:aws:s3::123456789012:accesspoint/example.mrap", "key");
    std::unordered_set<std::string> identifiers;
    for (const auto * key : {"key", "/key", "//key", "a/b", "a//b", "a%2Fb"})
    {
        DB::ObjectInfo object(key);
        const auto identity = DB::StorageObjectStorageSource::getUniqueStoragePathIdentifier(configuration, object, true);
        EXPECT_TRUE(identifiers.insert(identity).second);
        EXPECT_NE(identity.find(configuration.url.bucket), std::string::npos);
        EXPECT_EQ(DB::StorageObjectStorageSource::getUniqueStoragePathIdentifier(configuration, object, false),
            configuration.url.bucket + "/" + key);
    }
}

TEST(StorageS3MRAP, PathFilterCandidatesPreserveLiteralKeys)
{
    DB::StorageS3Configuration configuration;
    configuration.url = DB::S3::URI::fromMRAPArn(
        "arn:aws:s3::123456789012:accesspoint/example.mrap", "key");
    for (const auto * key : {"key", "/key", "//key", "a/b", "a//b", "a%2Fb"})
    {
        const auto path = DB::formatObjectPath(configuration, key, /*include_connection_info=*/false);
        EXPECT_EQ(DB::candidateKeysUnderPrefix(configuration.getNamespace(), path), DB::Strings{key});
        EXPECT_EQ(DB::joinPathUnderPrefix(configuration.getNamespace(), key), path);
    }
    EXPECT_TRUE(DB::candidateKeysUnderPrefix(configuration.getNamespace(), "other/key").empty());
}

TEST(StorageS3MRAP, PartitionedWriteAcceptsARNNamespace)
{
    auto configuration = std::make_shared<DB::StorageS3Configuration>();
    configuration->url = DB::S3::URI::fromMRAPArn(
        "arn:aws:s3::123456789012:accesspoint/example.mrap", "partition-{_partition_id}.csv");
    const auto name = configuration->getNamespace();
    EXPECT_NO_THROW(DB::PartitionedStorageObjectStorageSink::validateNamespace(name, configuration));

    auto ordinary_configuration = std::make_shared<DB::StorageS3Configuration>();
    EXPECT_ANY_THROW(DB::PartitionedStorageObjectStorageSink::validateNamespace(name, ordinary_configuration));
}

#endif
