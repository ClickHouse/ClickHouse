#include "config.h"

#if USE_AWS_S3
#include <gtest/gtest.h>
#include <unordered_set>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

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

#endif
