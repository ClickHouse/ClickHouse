#include <gtest/gtest.h>

#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/Cache/QueryConditionCache.h>

using namespace DB;

namespace
{

/// Two storage namespaces as `StorageObjectStorageConfiguration::getDataSourceDescription` reports
/// them: the endpoint together with the bucket.
const String namespace_a = "storage.example.com443/bucket_a";
const String namespace_b = "storage.example.com443/bucket_b";

ObjectInfo makeObjectInfo(const String & path, std::optional<String> etag, bool etag_is_strong)
{
    ObjectInfo object_info(path);
    if (etag.has_value())
    {
        ObjectMetadata metadata;
        metadata.etag = *etag;
        metadata.etag_is_strong = etag_is_strong;
        object_info.setObjectMetadata(metadata);
    }
    return object_info;
}

}

/// A strong etag (present and marked as a strong content identifier, e.g. S3/Azure) must key the
/// Query Condition Cache, so an in-place overwrite that changes the etag misses instead of reusing
/// stale row-group skip marks.
TEST(QueryConditionCacheKey, StrongEtagIsUsedAsKey)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", "strong-etag", /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false, namespace_a);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, QueryConditionCache::makeFilePartName("bucket/data.parquet", "strong-etag"));
}

/// A weak etag (present but not a strong content identifier, e.g. HDFS's second-precision
/// `(mtime, size)` token) must NOT key the cache: a same-second, same-size overwrite keeps the same
/// weak etag, so reusing the cached skip marks could silently drop matching rows. This is the
/// regression guarded here - if the guard reverts to `etag.empty()`, this test fails.
TEST(QueryConditionCacheKey, WeakEtagBypassesCache)
{
    auto object_info = makeObjectInfo("hdfs/data.parquet", "1700000000_42", /*etag_is_strong=*/ false);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false, namespace_a);
    EXPECT_FALSE(key.has_value());
}

/// A missing etag must also bypass the cache (fail-close), since the path alone is not a stable
/// identity for a mutable remote object.
TEST(QueryConditionCacheKey, EmptyEtagBypassesCache)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", "", /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false, namespace_a);
    EXPECT_FALSE(key.has_value());
}

/// Missing object metadata entirely must bypass the cache.
TEST(QueryConditionCacheKey, MissingMetadataBypassesCache)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", std::nullopt, /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false, namespace_a);
    EXPECT_FALSE(key.has_value());
}

/// A strong etag tracks an in-place rewrite, which `use_iceberg_manifest_object_metadata = 0` is
/// documented to protect against, so the key must follow it when the store supplied one.
TEST(QueryConditionCacheKey, DataLakePrefersAStrongEtag)
{
    auto object_info = makeObjectInfo("lake/data.parquet", "strong-etag", /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ true, namespace_a);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, QueryConditionCache::makeFilePartName("lake/data.parquet", "strong-etag"));

    auto rewritten = makeObjectInfo("lake/data.parquet", "strong-etag-after-rewrite", /*etag_is_strong=*/ true);
    EXPECT_NE(*key, *StorageObjectStorageSource::makeQueryConditionCacheKey(rewritten, /*is_data_lake=*/ true, namespace_a));
}

/// A data file answered from the manifest and one with no metadata yet must share an entry.
TEST(QueryConditionCacheKey, DataLakeManifestMetadataAgreesWithTheNamespaceFallback)
{
    ObjectInfo from_manifest("lake/data.parquet");
    ObjectMetadata metadata;
    metadata.immutable_contents_namespace = namespace_a;
    from_manifest.setObjectMetadata(metadata);

    auto without_metadata = makeObjectInfo("lake/data.parquet", std::nullopt, /*etag_is_strong=*/ true);

    EXPECT_EQ(
        *StorageObjectStorageSource::makeQueryConditionCacheKey(from_manifest, /*is_data_lake=*/ true, namespace_a),
        *StorageObjectStorageSource::makeQueryConditionCacheKey(without_metadata, /*is_data_lake=*/ true, namespace_a));
}

/// With a weak etag or no metadata, the storage namespace stands in as the content token.
TEST(QueryConditionCacheKey, DataLakeUsesTheNamespaceWithoutEtag)
{
    const auto expected = QueryConditionCache::makeFilePartName("lake/data.parquet", makeImmutableContentsCacheToken(namespace_a));

    auto weak = makeObjectInfo("lake/data.parquet", "1700000000_42", /*etag_is_strong=*/ false);
    auto weak_key = StorageObjectStorageSource::makeQueryConditionCacheKey(weak, /*is_data_lake=*/ true, namespace_a);
    ASSERT_TRUE(weak_key.has_value());
    EXPECT_EQ(*weak_key, expected);

    auto no_meta = makeObjectInfo("lake/data.parquet", std::nullopt, /*etag_is_strong=*/ true);
    auto no_meta_key = StorageObjectStorageSource::makeQueryConditionCacheKey(no_meta, /*is_data_lake=*/ true, namespace_a);
    ASSERT_TRUE(no_meta_key.has_value());
    EXPECT_EQ(*no_meta_key, expected);
}

/// A data-lake path is stripped of its bucket (`s3://bucket/tbl/data/x.parquet` becomes
/// `tbl/data/x.parquet`), and an object-storage table function reads under a nil table UUID, so the
/// table UUID in the cache key does not separate two tables the way it does for `MergeTree`. The
/// namespace has to, or one table's skip marks could be served for another table's data file.
TEST(QueryConditionCacheKey, DataLakeSeparatesTheNamespaces)
{
    auto object_info = makeObjectInfo("tbl/data/00001.parquet", std::nullopt, /*etag_is_strong=*/ true);

    const auto key_in_a = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ true, namespace_a);
    const auto key_in_b = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ true, namespace_b);

    ASSERT_TRUE(key_in_a.has_value());
    ASSERT_TRUE(key_in_b.has_value());
    EXPECT_NE(*key_in_a, *key_in_b);

    /// And the same data file in the same namespace still hits its own entry.
    EXPECT_EQ(
        *key_in_a,
        *StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ true, namespace_a));
}
