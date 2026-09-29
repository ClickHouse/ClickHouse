#include <gtest/gtest.h>

#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/Cache/QueryConditionCache.h>

using namespace DB;

namespace
{

/// Namespaces as `getDataSourceDescription` reports them: endpoint plus bucket.
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

ObjectInfo makeImmutableObjectInfo(const String & path, const String & storage_namespace)
{
    ObjectInfo object_info(path);
    ObjectMetadata metadata;
    metadata.immutable_contents_namespace = storage_namespace;
    object_info.setObjectMetadata(metadata);
    return object_info;
}

}

/// A strong etag (present and marked as a strong content identifier, e.g. S3/Azure) must key the
/// Query Condition Cache, so an in-place overwrite that changes the etag misses instead of reusing
/// stale row-group skip marks.
TEST(QueryConditionCacheKey, StrongEtagIsUsedAsKey)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", "strong-etag", /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, QueryConditionCache::makeFilePartName("bucket/data.parquet", "strong-etag"));
}

/// A weak etag (present but not a strong content identifier, e.g. HDFS's second-precision
/// `(mtime, size)` token) must NOT key the cache: a same-second, same-size overwrite keeps the same
/// weak etag, so reusing the cached skip marks could silently drop matching rows. This is the
/// regression guarded here - if the guard reverts to `etag.empty()`, this test fails. It covers a
/// data-lake read too: `IcebergHDFS` with the manifest shortcut off fetches exactly this metadata.
TEST(QueryConditionCacheKey, WeakEtagBypassesCache)
{
    auto object_info = makeObjectInfo("hdfs/data.parquet", "1700000000_42", /*etag_is_strong=*/ false);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info);
    EXPECT_FALSE(key.has_value());
}

/// A missing etag must also bypass the cache (fail-close), since the path alone is not a stable
/// identity for a mutable remote object.
TEST(QueryConditionCacheKey, EmptyEtagBypassesCache)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", "", /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info);
    EXPECT_FALSE(key.has_value());
}

/// Missing object metadata entirely must bypass the cache.
TEST(QueryConditionCacheKey, MissingMetadataBypassesCache)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", std::nullopt, /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info);
    EXPECT_FALSE(key.has_value());
}

/// The manifest shortcut records no etag but marks the file immutable within its namespace, and that
/// token keys the cache. Not the bare path: a data-lake path is bucket-relative, and a table function
/// reads under a nil table UUID.
TEST(QueryConditionCacheKey, ImmutableMetadataUsesTheNamespaceToken)
{
    auto object_info = makeImmutableObjectInfo("lake/data.parquet", namespace_a);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, QueryConditionCache::makeFilePartName("lake/data.parquet", makeImmutableContentsCacheToken(namespace_a)));
}

/// Two buckets holding the same relative path must not share skip marks.
TEST(QueryConditionCacheKey, ImmutableMetadataSeparatesTheNamespaces)
{
    auto in_a = makeImmutableObjectInfo("tbl/data/00001.parquet", namespace_a);
    auto in_b = makeImmutableObjectInfo("tbl/data/00001.parquet", namespace_b);

    const auto key_in_a = StorageObjectStorageSource::makeQueryConditionCacheKey(in_a);
    const auto key_in_b = StorageObjectStorageSource::makeQueryConditionCacheKey(in_b);
    ASSERT_TRUE(key_in_a.has_value());
    ASSERT_TRUE(key_in_b.has_value());
    EXPECT_NE(*key_in_a, *key_in_b);
    EXPECT_EQ(
        *key_in_a,
        *StorageObjectStorageSource::makeQueryConditionCacheKey(makeImmutableObjectInfo("tbl/data/00001.parquet", namespace_a)));
}

/// When the store answered with a strong etag it tracks an in-place rewrite, which
/// `use_iceberg_manifest_object_metadata = 0` is documented to protect against, so it wins.
TEST(QueryConditionCacheKey, StrongEtagWinsOverTheNamespace)
{
    auto object_info = makeImmutableObjectInfo("lake/data.parquet", namespace_a);
    auto metadata = *object_info.getObjectMetadata();
    metadata.etag = "strong-etag";
    object_info.setObjectMetadata(metadata);

    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, QueryConditionCache::makeFilePartName("lake/data.parquet", "strong-etag"));
}
