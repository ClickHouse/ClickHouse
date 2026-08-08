#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

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
    auto key = IcebergMetadataFilesCache::getKey("backend1", "uuid-123", "path/to/metadata.json");
    EXPECT_EQ(key, (std::string{"backend1\0uuid-123\0path/to/metadata.json", 39}));
}

TEST(IcebergMetadataCache, GetKeyDifferentUuidsSamePathProduceDifferentKeys)
{
    auto key1 = IcebergMetadataFilesCache::getKey("backend1", "uuid-aaa", "meta/v1.metadata.json");
    auto key2 = IcebergMetadataFilesCache::getKey("backend1", "uuid-bbb", "meta/v1.metadata.json");
    EXPECT_NE(key1, key2);
}

TEST(IcebergMetadataCache, GetKeySameUuidDifferentPathsProduceDifferentKeys)
{
    auto key1 = IcebergMetadataFilesCache::getKey("backend1", "uuid-123", "meta/v1.metadata.json");
    auto key2 = IcebergMetadataFilesCache::getKey("backend1", "uuid-123", "meta/v2.metadata.json");
    EXPECT_NE(key1, key2);
}

TEST(IcebergMetadataCache, GetKeyIsCollisionFree)
{
    // Different (uuid, path) pairs must never produce the same key.
    // Without a delimiter, ("a", "bc") and ("ab", "c") would collide.
    auto key1 = IcebergMetadataFilesCache::getKey("backend1", "a", "bc");
    auto key2 = IcebergMetadataFilesCache::getKey("backend1", "ab", "c");
    EXPECT_NE(key1, key2);
}

TEST(IcebergMetadataCache, GetKeyDifferentBackendsSameUuidSamePathProduceDifferentKeys)
{
    // Regression test for the cross-backend cache collision: two different physical backends
    // (e.g. different `S3` endpoints or `Azure` storage accounts) sharing the same bucket/container
    // name and table path must never collide on the same cache entry, even under a stale or
    // attacker-influenced `catalog_uuid_hint`.
    auto key1 = IcebergMetadataFilesCache::getKey("s3-endpoint-a/bucket", "uuid-123", "meta/v1.metadata.json");
    auto key2 = IcebergMetadataFilesCache::getKey("s3-endpoint-b/bucket", "uuid-123", "meta/v1.metadata.json");
    EXPECT_NE(key1, key2);
}

TEST(IcebergMetadataCache, MissCallsLoader)
{
    auto cache = makeCache();
    int load_count = 0;
    auto key = IcebergMetadataFilesCache::getKey("backend1", "uuid-1", "v1.metadata.json");

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
    auto key = IcebergMetadataFilesCache::getKey("backend1", "uuid-1", "v1.metadata.json");
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
    auto key_v1 = IcebergMetadataFilesCache::getKey("backend1", "uuid-1", "meta/v1.metadata.json");
    cache.getOrSetTableMetadata(key_v1, [&]() -> String { ++load_count; return "v1-json"; });

    // Table updated: v2 is a different path → must be a cache miss
    auto key_v2 = IcebergMetadataFilesCache::getKey("backend1", "uuid-1", "meta/v2.metadata.json");
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
    auto key = IcebergMetadataFilesCache::getKey("backend1", uuid, path);
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

    auto key_a = IcebergMetadataFilesCache::getKey("backend1", "uuid-aaa", path);
    auto key_b = IcebergMetadataFilesCache::getKey("backend1", "uuid-bbb", path);

    cache.getOrSetTableMetadata(key_a, [&]() -> String { ++load_count; return "json-a"; });
    auto result_b = cache.getOrSetTableMetadata(key_b, [&]() -> String { ++load_count; return "json-b"; });

    EXPECT_EQ(load_count, 2);
    EXPECT_EQ(result_b, "json-b");

    // Verify key_a is still cached correctly
    auto result_a = cache.getOrSetTableMetadata(key_a, [&]() -> String { ++load_count; return "wrong"; });
    EXPECT_EQ(load_count, 2); // no new load
    EXPECT_EQ(result_a, "json-a");
}

TEST(IcebergMetadataCache, LocationMatchesTableRootExact)
{
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("s3://bucket/ns/table", "bucket", "ns/table", "s3"));
}

TEST(IcebergMetadataCache, LocationMatchesTableRootWithTrailingSlashOnTableRoot)
{
    // The storage engine's configured path carries a trailing slash; the Iceberg
    // `location` field never does. Regression test for a bug where every warm
    // cache hit was rejected because of this mismatch (see test_metadata_cache
    // integration test).
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("s3://bucket/ns/table", "bucket", "ns/table/", "s3"));
}

TEST(IcebergMetadataCache, LocationMatchesTableRootWithTrailingSlashOnCachedLocation)
{
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("s3://bucket/ns/table/", "bucket", "ns/table", "s3"));
}

TEST(IcebergMetadataCache, LocationMatchesTableRootWithTrailingSlashesOnBoth)
{
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("s3://bucket/ns/table/", "bucket", "ns/table/", "s3"));
}

TEST(IcebergMetadataCache, LocationDoesNotMatchDifferentTableRoot)
{
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("s3://bucket/ns/other_table", "bucket", "ns/table", "s3"));
}

TEST(IcebergMetadataCache, LocationDoesNotMatchDifferentBucketWithSameKey)
{
    // Regression test: a suffix-only match would wrongly accept a same-named key living in a
    // different bucket. A stale `catalog_uuid_hint` colliding with another table's UUID must not
    // be accepted just because the trailing path segments happen to coincide.
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("s3://backup-bucket/ns/table", "bucket", "ns/table", "s3"));
}

TEST(IcebergMetadataCache, LocationMatchesWhenNamespaceAndTableRootAreEmpty)
{
    // Nothing to validate against (e.g. HDFS/Local backends with no bucket concept and an
    // empty root), so the check is permissive rather than rejecting every hit.
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("s3://bucket/ns/table", "", "", "s3"));
}

TEST(IcebergMetadataCache, LocationMatchesAbsolutePathWithNoScheme)
{
    // ClickHouse writes `location` as an absolute path (no scheme) for namespace-less backends
    // (HDFS/Local). Regression test: the leading slash must be trimmed from `cached_location`
    // the same way it already is from `table_root`, or a valid warm hit is rejected.
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("/warehouse/table", "", "warehouse/table", "local"));
}

TEST(IcebergMetadataCache, LocationDoesNotMatchAbsolutePathWithDifferentRoot)
{
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("/warehouse/other_table", "", "warehouse/table", "local"));
}

TEST(IcebergMetadataCache, LocationMatchesAuthorityBearingAzureUri)
{
    // Spark/Azure locations carry the container in an authority-bearing form
    // ("container@account.blob.core.windows.net"), not a bare namespace equal to
    // `StorageAzureConfiguration::getNamespace()`. The namespace must still be recognized as the
    // leading authority component.
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot(
        "wasb://container@account.blob.core.windows.net/ns/table", "container", "ns/table", "azure"));
}

TEST(IcebergMetadataCache, LocationMatchesHdfsUriWithHostAuthority)
{
    // `StorageHDFSConfiguration::getNamespace()` is always empty, but external writers commonly
    // store locations as `hdfs://namenode:8020/...`. A namespace-less backend has nothing to
    // validate the authority against, so any authority must be accepted as long as the key path
    // matches, or the UUID-hint fast path never works for HDFS-backed tables.
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("hdfs://namenode:8020/warehouse/table", "", "warehouse/table", "hdfs"));
}

TEST(IcebergMetadataCache, LocationMatchesHdfsUriWithNameserviceAuthority)
{
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("hdfs://user@nameservice/warehouse/table", "", "warehouse/table", "hdfs"));
}

TEST(IcebergMetadataCache, LocationDoesNotMatchAuthorityBearingUriWithDifferentContainer)
{
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot(
        "wasb://other-container@account.blob.core.windows.net/ns/table", "container", "ns/table", "azure"));
}

TEST(IcebergMetadataCache, LocationDoesNotMatchSchemelessLocalWhenCachedLocationHasAScheme)
{
    // Regression test: without a backend check, a Local table with an empty namespace would
    // accept any scheme-bearing location whose key path happens to match, because the empty
    // namespace was treated as "nothing to validate". A stale `catalog_uuid_hint` must not let an
    // S3 or HDFS table's cached metadata.json be reused by a Local table sharing the same path.
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("s3://bucket/warehouse/table", "", "warehouse/table", "local"));
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("hdfs://nn/warehouse/table", "", "warehouse/table", "local"));
}

TEST(IcebergMetadataCache, LocationDoesNotMatchAzureWhenBucketNameCoincidesWithS3Backend)
{
    // Regression test: `authority.starts_with(table_namespace + "@")` alone would accept an Azure
    // `wasb://bucket@account.../...` location for an S3 table whose bucket is also named "bucket",
    // even though they are unrelated backends and unrelated tables.
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot(
        "wasb://bucket@account.blob.core.windows.net/ns/table", "bucket", "ns/table", "s3"));
}

TEST(IcebergMetadataCache, DeriveTableNamespacePrefersNonEmptyConfigurationNamespace)
{
    EXPECT_EQ(Iceberg::deriveTableNamespaceForLocationCheck("bucket", "s3://bucket/ns/table"), "bucket");
}

TEST(IcebergMetadataCache, DeriveTableNamespaceFallsBackToRawUriAuthorityForHdfs)
{
    // `StorageHDFSConfiguration::getNamespace()` is always empty, but the table identity still
    // includes the namenode/nameservice from `getRawURI()`. Without this fallback, a stale
    // `catalog_uuid_hint` could reuse another HDFS cluster's cached metadata.json whenever both
    // tables share the same key path.
    EXPECT_EQ(Iceberg::deriveTableNamespaceForLocationCheck("", "hdfs://namenode:8020/warehouse/table"), "namenode:8020");
    EXPECT_EQ(Iceberg::deriveTableNamespaceForLocationCheck("", "hdfs://user@nameservice/warehouse/table"), "user@nameservice");
}

TEST(IcebergMetadataCache, DeriveTableNamespaceIsEmptyForSchemelessRawUri)
{
    // Local has no cluster identity to validate, so the result stays permissively empty.
    EXPECT_EQ(Iceberg::deriveTableNamespaceForLocationCheck("", "/warehouse/table"), "");
}

TEST(IcebergMetadataCache, LocationDoesNotMatchDifferentHdfsAuthorityWithSamePath)
{
    // Regression test for the cross-cluster collision: two different HDFS namenodes serving the
    // same `/warehouse/table` path must not be treated as the same table.
    const auto table_namespace = Iceberg::deriveTableNamespaceForLocationCheck("", "hdfs://nn1:8020/warehouse/table");
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("hdfs://nn2:8020/warehouse/table", table_namespace, "warehouse/table", "hdfs"));
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("hdfs://nn1:8020/warehouse/table", table_namespace, "warehouse/table", "hdfs"));
}

TEST(IcebergMetadataCache, LocationDoesNotMatchSchemelessDefaultWriteWhenNamespaceIsUnverifiable)
{
    // With `write_full_path_in_iceberg_metadata = 0` (the default), ClickHouse writes a schemeless
    // `location` regardless of backend. Two different tables in different buckets/containers with
    // the same key path would then produce the *same* schemeless location, so a stale
    // `catalog_uuid_hint` colliding with another table's UUID must not be accepted just because
    // the key path matches: a schemeless location carries no authority to check `table_namespace`
    // against, so it must miss (fall back to a cold read) whenever there is a namespace to verify.
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("warehouse/table", "bucket", "warehouse/table", "s3"));
    EXPECT_FALSE(Iceberg::cachedLocationMatchesTableRoot("/warehouse/table", "container", "warehouse/table", "azure"));
}

TEST(IcebergMetadataCache, LocationMatchesSchemelessDefaultWriteWhenNamespaceIsEmpty)
{
    // When there is genuinely nothing to validate (e.g. HDFS with no derivable raw-URI authority,
    // or Local), a schemeless location is still accepted.
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("/warehouse/table", "", "warehouse/table", "hdfs"));
}

TEST(IcebergMetadataCache, LocationMatchesFileGsOssSchemeEquivalents)
{
    // Mirrors DataLake::parseStorageTypeFromString's equivalences: `file` -> Local, `gs`/`oss` -> S3.
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("file:///warehouse/table", "", "warehouse/table", "local"));
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("gs://bucket/ns/table", "bucket", "ns/table", "s3"));
    EXPECT_TRUE(Iceberg::cachedLocationMatchesTableRoot("oss://bucket/ns/table", "bucket", "ns/table", "s3"));
}

#endif
