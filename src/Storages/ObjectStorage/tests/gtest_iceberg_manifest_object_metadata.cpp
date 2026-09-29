#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>

using namespace DB;

namespace
{

/// Namespaces as `getDataSourceDescription` reports them: endpoint plus bucket.
const String namespace_a = "storage.example.com443/bucket_a";
const String namespace_b = "storage.example.com443/bucket_b";

/// Built the way a cluster function worker builds it: path first, `info` assigned after.
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
    /// Without a token the content caches disable themselves.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->getContentCacheToken().has_value());
}

TEST(IcebergManifestObjectMetadata, TheStorageNamespaceIdentifiesTheContents)
{
    /// An empty token would leave the bucket-relative path as the whole identity.
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
    /// `resolve` strips the bucket, so two tables can hold this relative path.
    const auto in_a = makeObject("table/data/00001.parquet", 4096);
    const auto in_b = makeObject("table/data/00001.parquet", 4096);

    EXPECT_NE(
        in_a.tryGetObjectMetadataWithoutRequest(namespace_a)->getContentCacheToken(),
        in_b.tryGetObjectMetadataWithoutRequest(namespace_b)->getContentCacheToken());
}

TEST(IcebergManifestObjectMetadata, TheSamePathInOneNamespaceIsTheSameContent)
{
    /// Separating namespaces must not cost the cache hit this path exists to keep.
    const auto first_read = makeObject("table/data/00001.parquet", 4096);
    const auto second_read = makeObject("table/data/00001.parquet", 4096);

    EXPECT_EQ(
        first_read.tryGetObjectMetadataWithoutRequest(namespace_a)->getContentCacheToken(),
        second_read.tryGetObjectMetadataWithoutRequest(namespace_a)->getContentCacheToken());
}

TEST(IcebergManifestObjectMetadata, AnImmutableTokenCannotCollideWithAnEtag)
{
    /// Both tokens share one key field; the prefix keeps a namespace from aliasing an ETag.
    ObjectMetadata with_etag;
    with_etag.etag = namespace_a;

    const auto object = makeObject("db/table/data/file.parquet", 4096);
    const auto immutable = object.tryGetObjectMetadataWithoutRequest(namespace_a);

    EXPECT_NE(with_etag.getContentCacheToken(), immutable->getContentCacheToken());
}

TEST(IcebergManifestObjectMetadata, AnEtagTakesPrecedenceOverTheNamespace)
{
    /// When the store answered, the ETag stays the token and existing entries keep their keys.
    ObjectMetadata metadata;
    metadata.etag = "0123456789abcdef0123456789abcdef";
    metadata.immutable_contents_namespace = namespace_a;

    EXPECT_EQ(metadata.getContentCacheToken(), metadata.etag);
}

TEST(IcebergManifestObjectMetadata, CarriesNoEtagAndIsNotThePlaceholder)
{
    /// No ETag, so S3 read validation sends no `If-Match` that would match nothing. `is_fetched` is
    /// true: this is complete metadata, not the placeholder.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->etag.empty());
    EXPECT_FALSE(metadata->isEtagUsableAsCacheKey());
    EXPECT_TRUE(metadata->is_fetched);
}

TEST(IcebergManifestObjectMetadata, ModificationTimeIsReportedAsUnknown)
{
    /// A default `last_modified` presented as known would look older than any cached count.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest(namespace_a);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_FALSE(metadata->is_last_modified_known);
}

TEST(IcebergManifestObjectMetadata, FallsBackWhenTheManifestRecordedNoSize)
{
    const auto object = makeObject("db/table/data/file.parquet", std::nullopt);

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

TEST(IcebergManifestObjectMetadata, FallsBackOnANegativeSize)
{
    /// `file_size_in_bytes` is signed in Avro; a negative value must not become a huge unsigned size.
    const auto object = makeObject("db/table/data/file.parquet", -1);

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

TEST(IcebergManifestObjectMetadata, AZeroByteFileIsLeftToTheObjectStore)
{
    /// With `skip_empty_files`, a missing object answered from the manifest would pass for an empty one.
    const auto object = makeObject("db/table/data/empty.parquet", 0);

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

TEST(IcebergManifestObjectMetadata, PlainObjectInfoKnowsNothingWithoutARequest)
{
    const ObjectInfo object(RelativePathWithMetadata{"bucket/key.parquet"});

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest(namespace_a).has_value());
}

#endif
