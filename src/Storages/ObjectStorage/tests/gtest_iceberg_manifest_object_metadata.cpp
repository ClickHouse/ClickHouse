#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>

using namespace DB;

namespace
{

/// Two storage namespaces as `StorageObjectStorageConfiguration::getDataSourceDescription` reports
/// them: the endpoint together with the bucket. `IcebergPathResolver::resolve` strips both from a
/// data file's path, so they are the only thing separating the same relative path in two tables.
const String namespace_a = "storage.example.com443/bucket_a";
const String namespace_b = "storage.example.com443/bucket_b";

/// Mirrors how a cluster function worker builds the object: the path-only constructor, with `info`
/// assigned afterwards. Deriving the metadata lazily is what makes it work on that path too.
IcebergDataObjectInfo makeObject(const String & path, std::optional<Int64> file_size_in_bytes)
{
    IcebergDataObjectInfo object(RelativePathWithMetadata{path});
    object.info.file_size_in_bytes = file_size_in_bytes;
    return object;
}

}

TEST(IcebergManifestObjectMetadata, UsesManifestRecordedSize)
{
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_EQ(metadata->size_bytes, 4096u);
    EXPECT_TRUE(metadata->is_size_known);
}

TEST(IcebergManifestObjectMetadata, ContentIsIdentifiedSoContentCachesStayEnabled)
{
    /// The Parquet metadata cache, the filesystem cache and the page cache all disable themselves
    /// for an object whose contents cannot be identified.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->getContentCacheToken().has_value());
}

TEST(IcebergManifestObjectMetadata, TheStorageNamespaceIdentifiesTheContents)
{
    /// A data file is immutable, so it needs no ETag - but its path is relative to the bucket, so
    /// the namespace has to stand in for one. An empty token would leave the relative path as the
    /// whole identity in caches that are shared by every table on the server.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    ASSERT_TRUE(metadata->immutable_contents_namespace.has_value());
    EXPECT_EQ(*metadata->immutable_contents_namespace, namespace_a);
    EXPECT_EQ(metadata->getContentCacheToken(), makeImmutableContentsCacheToken(namespace_a));
    EXPECT_FALSE(metadata->getContentCacheToken()->empty());
}

TEST(IcebergManifestObjectMetadata, TheSamePathInTwoNamespacesIsNotTheSameContent)
{
    /// `IcebergPathResolver::resolve` strips the bucket, so two tables can hold this same relative
    /// path. Sharing a token would let a read of one serve the other's cached footer or bytes.
    const auto in_a = makeObject("table/data/00001.parquet", 4096);
    const auto in_b = makeObject("table/data/00001.parquet", 4096);

    EXPECT_NE(
        in_a.tryGetObjectMetadataWithoutRequest(namespace_a)->getContentCacheToken(),
        in_b.tryGetObjectMetadataWithoutRequest(namespace_b)->getContentCacheToken());
}

TEST(IcebergManifestObjectMetadata, TheSamePathInOneNamespaceIsTheSameContent)
{
    /// The other half of the contract: separating the namespaces must not cost the cache hit that
    /// this whole path exists to keep. Two reads of one data file still agree on its token.
    const auto first_read = makeObject("table/data/00001.parquet", 4096);
    const auto second_read = makeObject("table/data/00001.parquet", 4096);

    EXPECT_EQ(
        first_read.tryGetObjectMetadataWithoutRequest(namespace_a)->getContentCacheToken(),
        second_read.tryGetObjectMetadataWithoutRequest(namespace_a)->getContentCacheToken());
}

TEST(IcebergManifestObjectMetadata, AnImmutableTokenCannotCollideWithAnEtag)
{
    /// The two kinds of token share one field in every cache key, so a namespace that happened to
    /// read like an ETag must not alias one. The prefix is what keeps the value spaces apart.
    ObjectMetadata with_etag;
    with_etag.etag = namespace_a;

    const auto object = makeObject("db/table/data/file.parquet", 4096);
    const auto immutable = object.tryGetObjectMetadataWithoutRequest(namespace_a);

    EXPECT_NE(with_etag.getContentCacheToken(), immutable->getContentCacheToken());
}

TEST(IcebergManifestObjectMetadata, AnEtagTakesPrecedenceOverTheNamespace)
{
    /// When the store did answer, its ETag is the stronger identifier and stays the token, so the
    /// key of an already-cached entry does not change under this setting.
    ObjectMetadata metadata;
    metadata.etag = "0123456789abcdef0123456789abcdef";
    metadata.immutable_contents_namespace = namespace_a;

    EXPECT_EQ(metadata.getContentCacheToken(), metadata.etag);
}

TEST(IcebergManifestObjectMetadata, CarriesNoEtagAndIsNotThePlaceholder)
{
    /// No ETag, because the store returned nothing: that keeps this out of the S3 read-time
    /// validation, where it would be sent as `If-Match` and match nothing. `is_fetched` stays true
    /// because this metadata is complete, unlike the placeholder the read path fills in.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->etag.empty());
    EXPECT_FALSE(metadata->isEtagUsableAsCacheKey());
    EXPECT_TRUE(metadata->is_fetched);
}

TEST(IcebergManifestObjectMetadata, ModificationTimeIsReportedAsUnknown)
{
    /// The manifest records none, and a default `last_modified` presented as known would look older
    /// than any cached entry to the schema and count caches, which would then reuse a stale value.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_FALSE(metadata->is_last_modified_known);
}

TEST(IcebergManifestObjectMetadata, FallsBackWhenTheManifestRecordedNoSize)
{
    const auto object = makeObject("db/table/data/file.parquet", std::nullopt);

    /// No value means the caller asks the object store, exactly as before this change.
    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

TEST(IcebergManifestObjectMetadata, FallsBackOnANegativeSize)
{
    /// `file_size_in_bytes` is signed in the Avro schema, so a malformed manifest can carry a
    /// negative value. It must not be cast into a huge unsigned size.
    const auto object = makeObject("db/table/data/file.parquet", -1);

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

TEST(IcebergManifestObjectMetadata, AZeroByteFileIsLeftToTheObjectStore)
{
    /// `skip_empty_files` skips an empty object before anything touches it. Answered from the
    /// manifest alone, a missing object would then pass for an empty one instead of being reported,
    /// so a zero size keeps the metadata request that would notice it is gone.
    const auto object = makeObject("db/table/data/empty.parquet", 0);

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

TEST(IcebergManifestObjectMetadata, PlainObjectInfoKnowsNothingWithoutARequest)
{
    /// The default stays empty so every storage other than Iceberg keeps asking the object store.
    const ObjectInfo object(RelativePathWithMetadata{"bucket/key.parquet"});

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

#endif
