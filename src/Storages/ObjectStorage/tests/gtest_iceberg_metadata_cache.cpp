#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>

using namespace DB;

namespace
{

constexpr size_t MAX_CACHE_SIZE = 1024 * 1024; // 1 MB
constexpr size_t MAX_CACHE_COUNT = 100;
constexpr double SIZE_RATIO = 0.5;

IcebergMetadataFilesCache makeCache()
{
    return IcebergMetadataFilesCache("SLRU", MAX_CACHE_SIZE, MAX_CACHE_COUNT, SIZE_RATIO);
}

}

TEST(IcebergMetadataCache, GetKeyComposesUuidAndPath)
{
    auto key = IcebergMetadataFilesCache::getKey("uuid-123", "path/to/metadata.json");
    EXPECT_EQ(key, "uuid-123path/to/metadata.json");
}

TEST(IcebergMetadataCache, GetKeyDifferentUuidsSamePathProduceDifferentKeys)
{
    auto key1 = IcebergMetadataFilesCache::getKey("uuid-aaa", "meta/v1.metadata.json");
    auto key2 = IcebergMetadataFilesCache::getKey("uuid-bbb", "meta/v1.metadata.json");
    EXPECT_NE(key1, key2);
}

TEST(IcebergMetadataCache, GetKeySameUuidDifferentPathsProduceDifferentKeys)
{
    auto key1 = IcebergMetadataFilesCache::getKey("uuid-123", "meta/v1.metadata.json");
    auto key2 = IcebergMetadataFilesCache::getKey("uuid-123", "meta/v2.metadata.json");
    EXPECT_NE(key1, key2);
}

TEST(IcebergMetadataCache, MissCallsLoader)
{
    auto cache = makeCache();
    int load_count = 0;
    auto key = IcebergMetadataFilesCache::getKey("uuid-1", "v1.metadata.json");

    auto result = cache.getOrSetTableMetadata(key, [&]() -> String
    {
        ++load_count;
        return R"({"table-uuid":"uuid-1"})";
    });

    EXPECT_EQ(load_count, 1);
    EXPECT_EQ(result, R"({"table-uuid":"uuid-1"})");
}

TEST(IcebergMetadataCache, HitDoesNotCallLoader)
{
    auto cache = makeCache();
    int load_count = 0;
    auto key = IcebergMetadataFilesCache::getKey("uuid-1", "v1.metadata.json");
    auto loader = [&]() -> String { ++load_count; return R"({"table-uuid":"uuid-1"})"; };

    cache.getOrSetTableMetadata(key, loader);
    auto result = cache.getOrSetTableMetadata(key, loader);

    EXPECT_EQ(load_count, 1); // loader called only on first miss
    EXPECT_EQ(result, R"({"table-uuid":"uuid-1"})");
}

TEST(IcebergMetadataCache, NewSnapshotNewPathIsCacheMiss)
{
    auto cache = makeCache();
    int load_count = 0;

    // Populate cache for v1
    auto key_v1 = IcebergMetadataFilesCache::getKey("uuid-1", "meta/v1.metadata.json");
    cache.getOrSetTableMetadata(key_v1, [&]() -> String { ++load_count; return "v1-json"; });

    // Table updated: v2 is a different path → must be a cache miss
    auto key_v2 = IcebergMetadataFilesCache::getKey("uuid-1", "meta/v2.metadata.json");
    auto result = cache.getOrSetTableMetadata(key_v2, [&]() -> String { ++load_count; return "v2-json"; });

    EXPECT_EQ(load_count, 2); // both v1 and v2 triggered a load
    EXPECT_EQ(result, "v2-json");
}

TEST(IcebergMetadataCache, RetroactiveCachePopulationEnablesCacheHit)
{
    auto cache = makeCache();
    int load_count = 0;
    const String uuid = "uuid-abc";
    const String path = "meta/v1.metadata.json";
    String json = R"({"table-uuid":"uuid-abc","format-version":2})";

    // Simulate what IcebergMetadata::initializePersistentTableComponents does:
    // on first query UUID was unknown so we fetched without caching,
    // then retroactively populate the cache.
    auto key = IcebergMetadataFilesCache::getKey(uuid, path);
    cache.getOrSetTableMetadata(key, [&]() -> String { ++load_count; return json; });

    // Second query: UUID is now known → same key → cache hit, no load
    auto result = cache.getOrSetTableMetadata(key, [&]() -> String { ++load_count; return "should-not-be-called"; });

    EXPECT_EQ(load_count, 1);
    EXPECT_EQ(result, json);
}

TEST(IcebergMetadataCache, TablesWithSamePathButDifferentUuidsAreIndependent)
{
    auto cache = makeCache();
    int load_count = 0;
    const String path = "meta/v1.metadata.json";

    auto key_a = IcebergMetadataFilesCache::getKey("uuid-aaa", path);
    auto key_b = IcebergMetadataFilesCache::getKey("uuid-bbb", path);

    cache.getOrSetTableMetadata(key_a, [&]() -> String { ++load_count; return "json-a"; });
    auto result_b = cache.getOrSetTableMetadata(key_b, [&]() -> String { ++load_count; return "json-b"; });

    EXPECT_EQ(load_count, 2);
    EXPECT_EQ(result_b, "json-b");

    // Verify key_a is still cached correctly
    auto result_a = cache.getOrSetTableMetadata(key_a, [&]() -> String { ++load_count; return "wrong"; });
    EXPECT_EQ(load_count, 2); // no new load
    EXPECT_EQ(result_a, "json-a");
}
