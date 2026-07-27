#include <gtest/gtest.h>

#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/Cache/QueryConditionCache.h>

using namespace DB;

namespace
{

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
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false);
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
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false);
    EXPECT_FALSE(key.has_value());
}

/// A missing etag must also bypass the cache (fail-close), since the path alone is not a stable
/// identity for a mutable remote object.
TEST(QueryConditionCacheKey, EmptyEtagBypassesCache)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", "", /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false);
    EXPECT_FALSE(key.has_value());
}

/// Missing object metadata entirely must bypass the cache.
TEST(QueryConditionCacheKey, MissingMetadataBypassesCache)
{
    auto object_info = makeObjectInfo("bucket/data.parquet", std::nullopt, /*etag_is_strong=*/ true);
    auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(object_info, /*is_data_lake=*/ false);
    EXPECT_FALSE(key.has_value());
}

/// Data-lake data files are immutable, so the path is a stable identity on its own: the cache is
/// keyed on the identifier alone, even when the object carries a weak etag or no etag at all.
TEST(QueryConditionCacheKey, DataLakeUsesIdentifierWithoutEtag)
{
    auto weak = makeObjectInfo("lake/data.parquet", "1700000000_42", /*etag_is_strong=*/ false);
    auto weak_key = StorageObjectStorageSource::makeQueryConditionCacheKey(weak, /*is_data_lake=*/ true);
    ASSERT_TRUE(weak_key.has_value());
    EXPECT_EQ(*weak_key, "lake/data.parquet");

    auto no_meta = makeObjectInfo("lake/data.parquet", std::nullopt, /*etag_is_strong=*/ true);
    auto no_meta_key = StorageObjectStorageSource::makeQueryConditionCacheKey(no_meta, /*is_data_lake=*/ true);
    ASSERT_TRUE(no_meta_key.has_value());
    EXPECT_EQ(*no_meta_key, "lake/data.parquet");
}
