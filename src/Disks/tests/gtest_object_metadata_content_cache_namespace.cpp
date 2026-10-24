#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

using DB::ObjectMetadata;

/// The native GCS backend uses the object generation as its etag, and another bucket or endpoint can repeat it for an
/// object with the same key. The caches key on the path within the bucket and the content-cache token, so the token
/// must tell such objects apart.

TEST(ObjectMetadataContentCacheNamespace, EqualEtagsInDifferentNamespacesDoNotShareAToken)
{
    ObjectMetadata first;
    first.etag = "1700000000000000";
    first.content_cache_namespace = "gcs:/bucket-a";

    ObjectMetadata second = first;
    second.content_cache_namespace = "gcs:/bucket-b";

    ObjectMetadata third = first;
    third.content_cache_namespace = "gcs:http://emulator:4443/bucket-a";

    ASSERT_TRUE(first.getContentCacheToken().has_value());
    EXPECT_NE(first.getContentCacheToken(), second.getContentCacheToken());
    EXPECT_NE(first.getContentCacheToken(), third.getContentCacheToken());
}

TEST(ObjectMetadataContentCacheNamespace, NoNamespaceKeepsTheEtag)
{
    ObjectMetadata metadata;
    metadata.etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
    EXPECT_EQ(metadata.getContentCacheToken(), metadata.etag);
}

TEST(ObjectMetadataContentCacheNamespace, WeakEtagIsNotUsedEvenWithANamespace)
{
    ObjectMetadata metadata;
    metadata.etag = "1700000000000000";
    metadata.etag_is_strong = false;
    metadata.content_cache_namespace = "gcs:/bucket-a";
    EXPECT_FALSE(metadata.getContentCacheToken().has_value());
}
