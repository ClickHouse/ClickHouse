#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Common/Exception.h>

using namespace DB;
using Iceberg::BlobStorageDescription;
using Iceberg::IcebergPathFromMetadata;
using Iceberg::IcebergPathResolver;
using Iceberg::PositionDeleteMatchKey;
using Iceberg::PositionDeleteScope;

namespace
{

struct DeriveTableRootCase
{
    std::string_view name;
    String table_location;
    String queried_path;
    String metadata_file_key;
    String expected_table_root;
    IcebergPathResolver::RootRelation expected_relation;
};

void check(const DeriveTableRootCase & test_case)
{
    auto derivation = IcebergPathResolver::deriveTableRoot(
        test_case.table_location, test_case.queried_path, test_case.metadata_file_key);
    EXPECT_EQ(derivation.table_root, test_case.expected_table_root) << test_case.name;
    EXPECT_EQ(static_cast<int>(derivation.relation), static_cast<int>(test_case.expected_relation)) << test_case.name;
}

}

/// A storage path is absolute on some backends (HDFS always makes it so) while the `location` the
/// document declares carries a URI authority. The character preceding the shared tail is then part
/// of the authority rather than a path separator, so the two spellings still denote one directory.
TEST(IcebergDeriveTableRoot, AbsoluteStoragePath)
{
    check({"authority location", "hdfs://hdfs1:9000/warehouse/tbl", "/warehouse",
        "/warehouse/tbl/metadata/v2.metadata.json",
        "/warehouse/tbl", IcebergPathResolver::RootRelation::AdoptedDescendant});

    /// The tail matches only mid-component, so the two paths name different directories.
    check({"tail is not component-aligned", "hdfs://h:9000/xwarehouse/tbl", "/warehouse",
        "/warehouse/tbl/metadata/v2.metadata.json",
        "/warehouse", IcebergPathResolver::RootRelation::Unknown});
}

/// Every spelling in which a backend names one directory: what `location` carries in front of the
/// storage path is a scheme and an authority, which name a host rather than a directory.
TEST(IcebergDeriveTableRoot, LocationDiffersOnlyByAuthority)
{
    check({"hdfs", "hdfs://host:9000/tbl/UUIDDIR", "/tbl", "/tbl/UUIDDIR/metadata/v2.metadata.json",
        "/tbl/UUIDDIR", IcebergPathResolver::RootRelation::AdoptedDescendant});

    check({"abfss", "abfss://container@account.dfs.core.windows.net/tbl/UUIDDIR", "tbl",
        "tbl/UUIDDIR/metadata/v2.metadata.json",
        "tbl/UUIDDIR", IcebergPathResolver::RootRelation::AdoptedDescendant});

    check({"virtual-hosted s3", "https://mybucket.s3.amazonaws.com/tbl/UUIDDIR", "tbl",
        "tbl/UUIDDIR/metadata/v2.metadata.json",
        "tbl/UUIDDIR", IcebergPathResolver::RootRelation::AdoptedDescendant});

    check({"no scheme", "/tbl/UUIDDIR", "tbl", "tbl/UUIDDIR/metadata/v2.metadata.json",
        "tbl/UUIDDIR", IcebergPathResolver::RootRelation::AdoptedDescendant});

    check({"s3", "s3://warehouse/tbl/UUIDDIR", "tbl", "tbl/UUIDDIR/metadata/v2.metadata.json",
        "tbl/UUIDDIR", IcebergPathResolver::RootRelation::AdoptedDescendant});

    /// A Spark warehouse of many tables, queried at the warehouse instead of at one table.
    check({"spark warehouse", "s3a://spark-bucket/warehouse/db/spark_table", "warehouse",
        "warehouse/db/spark_table/metadata/v2.metadata.json",
        "warehouse/db/spark_table", IcebergPathResolver::RootRelation::AdoptedDescendant});
}

/// `location` ending with the document's own directory is a coincidence once a directory of its own
/// precedes it: the table then lives under a different prefix, and the queried path, which does
/// hold the document, stays the root that every file already resolves against.
TEST(IcebergDeriveTableRoot, LocationHasUnmatchedPathComponent)
{
    check({"stale copied location", "s3://old/backup/warehouse/db/t", "warehouse",
        "warehouse/db/t/metadata/v2.metadata.json",
        "warehouse", IcebergPathResolver::RootRelation::Unknown});

    check({"no scheme", "/backup/warehouse/db/t", "warehouse",
        "warehouse/db/t/metadata/v2.metadata.json",
        "warehouse", IcebergPathResolver::RootRelation::Unknown});

    check({"absolute storage path", "hdfs://h:9000/other/warehouse/tbl", "/warehouse",
        "/warehouse/tbl/metadata/v2.metadata.json",
        "/warehouse", IcebergPathResolver::RootRelation::Unknown});
}

TEST(IcebergDeriveTableRoot, RelativeStoragePath)
{
    check({"uri location", "s3://test/dir/t1/sub", "dir/t1", "dir/t1/sub/metadata/v2.metadata.json",
        "dir/t1/sub", IcebergPathResolver::RootRelation::AdoptedDescendant});

    check({"absolute location", "/dir/t1/sub", "dir/t1", "dir/t1/sub/metadata/v2.metadata.json",
        "dir/t1/sub", IcebergPathResolver::RootRelation::AdoptedDescendant});

    /// `location` names the queried path itself, so it contradicts the document's position and
    /// neither source can be trusted over the other.
    check({"location shallower than the document", "/dir/t4", "dir/t4",
        "dir/t4/archive/metadata/v2.metadata.json",
        "dir/t4", IcebergPathResolver::RootRelation::Unknown});

    check({"document at the queried path", "/dir/t3/sub", "dir/t3", "dir/t3/metadata/v2.metadata.json",
        "dir/t3", IcebergPathResolver::RootRelation::Same});

    check({"no location", "", "dir/t1", "dir/t1/sub/metadata/v2.metadata.json",
        "dir/t1", IcebergPathResolver::RootRelation::Unknown});
}

/// The storage root is not a table root: comparing the two empty tails would otherwise agree.
TEST(IcebergDeriveTableRoot, EmptyTailIsNotARoot)
{
    check({"storage root", "", "", "//metadata/v2.metadata.json",
        "", IcebergPathResolver::RootRelation::Unknown});
}

/// Hadoop writes `file:/x`, a URI with no authority.
TEST(IcebergDeriveTableRoot, HadoopFileLocation)
{
    check({"file:/", "file:/abs/w/db/t", "/abs/w", "/abs/w/db/t/metadata/v1.metadata.json",
        "/abs/w/db/t", IcebergPathResolver::RootRelation::AdoptedDescendant});

    /// `tbl` is the bucket of this location, not a directory.
    check({"authority is the first directory", "s3://tbl/UUIDDIR", "tbl", "tbl/UUIDDIR/metadata/v2.metadata.json",
        "tbl", IcebergPathResolver::RootRelation::Unknown});
}

namespace
{

BlobStorageDescription localStorage()
{
    return {.type_name = "local", .namespace_name = "", .allow_foreign_namespaces = false, .is_filesystem = true};
}

BlobStorageDescription hdfsStorage()
{
    return {.type_name = "hdfs", .namespace_name = "", .allow_foreign_namespaces = false, .is_filesystem = true};
}

BlobStorageDescription s3Storage(bool allow_foreign_namespaces = false)
{
    return {.type_name = "s3", .namespace_name = "bucket", .allow_foreign_namespaces = allow_foreign_namespaces, .is_filesystem = false};
}

BlobStorageDescription azureStorage()
{
    return {.type_name = "azure", .namespace_name = "c", .allow_foreign_namespaces = false, .is_filesystem = false};
}

IcebergPathFromMetadata metadataPath(const String & path)
{
    return IcebergPathFromMetadata::deserialize(path);
}

struct ResolveCase
{
    std::string_view name;
    String table_location;
    String table_root;
    BlobStorageDescription storage;
    String path;
    /// `std::nullopt` if the path cannot be resolved.
    std::optional<String> expected;
};

void check(const ResolveCase & test_case)
{
    IcebergPathResolver resolver(test_case.table_location, test_case.table_root, test_case.storage);
    if (test_case.expected)
        EXPECT_EQ(resolver.resolve(metadataPath(test_case.path)), *test_case.expected) << test_case.name;
    else
        EXPECT_THROW(resolver.resolve(metadataPath(test_case.path)), Exception) << test_case.name;
}

}

/// What the resolver returned before paths were compared by component.
TEST(IcebergPathResolver, Unchanged)
{
    check({"s3", "s3://bucket/warehouse/tbl", "warehouse/tbl", s3Storage(),
        "s3://bucket/warehouse/tbl/data/a.parquet", "warehouse/tbl/data/a.parquet"});
    check({"s3, outside the location, containing the table root", "s3://bucket/warehouse/tbl", "warehouse/tbl", s3Storage(),
        "s3://bucket/data/warehouse/tbl/x.parquet", "warehouse/tbl/x.parquet"});
    check({"hdfs", "hdfs://nn:9000/warehouse/tbl", "/warehouse/tbl", hdfsStorage(),
        "hdfs://nn:9000/warehouse/tbl/data/a", "/warehouse/tbl/data/a"});
    check({"hdfs, outside the location", "hdfs://nn:9000/warehouse/tbl", "/warehouse/tbl", hdfsStorage(),
        "hdfs://nn:9000/other/x", "/other/x"});
    check({"abfss", "abfss://c@acc.dfs.core.windows.net/tbl", "tbl", azureStorage(),
        "abfss://c@acc.dfs.core.windows.net/tbl/data/a", "tbl/data/a"});
    check({"written by ClickHouse", "/tbl", "tbl", localStorage(), "/tbl/data/a", "tbl/data/a"});
    check({"moved table", "/tbl", "copy", localStorage(), "/tbl/data/a", "copy/data/a"});
    check({"s3a path, s3 location", "s3://bucket/tbl", "tbl", s3Storage(), "s3a://bucket/tbl/data/a", "tbl/data/a"});
    check({"the location itself", "/tbl", "tbl", localStorage(), "/tbl", "tbl/"});
}

/// The tables of the external test suites in issue #129990.
TEST(IcebergPathResolver, SpelledDifferently)
{
    check({"file:/ path, file:/// location", "file:///test-warehouse/iceberg_v3_spark_variant", "impala/iceberg_v3_spark_variant",
        localStorage(), "file:/test-warehouse/iceberg_v3_spark_variant/metadata/snap-1.avro",
        "impala/iceberg_v3_spark_variant/metadata/snap-1.avro"});
    check({"file:/ path, no scheme in location", "/tmp/iceberg/nation", "dremio/nation", localStorage(),
        "file:/tmp/iceberg/nation/metadata/x.avro", "dremio/nation/metadata/x.avro"});
    check({"file:/// path, no scheme in location", "/tmp/iceberg/nation", "dremio/nation", localStorage(),
        "file:///tmp/iceberg/nation/data/y.parquet", "dremio/nation/data/y.parquet"});
    check({"default file system of the writer", "/test-warehouse/t", "w/t", localStorage(),
        "hdfs://localhost:20500/test-warehouse/t/data/a.parquet", "w/t/data/a.parquet"});

    IcebergPathResolver hadoop("file:/tmp/t", "t", localStorage());
    EXPECT_EQ(hadoop.getTableLocation(), "file:/tmp/t");
    EXPECT_EQ(hadoop.resolve(metadataPath("file:/tmp/t/data/a")), "t/data/a");
    EXPECT_EQ(hadoop.resolve(metadataPath("/tmp/t/data/a")), "t/data/a");
    EXPECT_EQ(
        hadoop.resolveForCatalog(metadataPath("file:/x/metadata/v1.metadata.json")), "file:/x/metadata/v1.metadata.json");
}

/// Paths outside the location are relative to the common parent directory of the table and the file.
TEST(IcebergPathResolver, OutsideLocation)
{
    const String column_mapping = "data/persistent/column_mapping/warehouse/default.db/my_table";
    const String mock_data = "data/persistent/column_mapping/warehouse/example/mock_data.parquet";
    check({"relative location", column_mapping, column_mapping, localStorage(), mock_data, mock_data});
    check({"relative location under the root", column_mapping, "uniq/" + column_mapping, localStorage(), mock_data, "uniq/" + mock_data});
    check({"relative location, absolute root", column_mapping, "/abs/" + column_mapping, localStorage(), mock_data, "/abs/" + mock_data});
    check({"data in the parent directory", "/w/v/iceberg", "cxx/v/iceberg", localStorage(), "/w/v/data/a.parquet",
        "cxx/v/data/a.parquet"});
    check({"sibling table", "/w/db/t1", "tea/db/t1", localStorage(), "/w/db/t2/metadata/snap.avro", "tea/db/t2/metadata/snap.avro"});
    check({"s3a location, s3 path", "s3a://bucket/doris/multi_partition2", "doris/multi_partition2", s3Storage(),
        "s3://bucket/data/doris_x/a.parquet", "data/doris_x/a.parquet"});
    check({"table root longer than the location", "/t", "very/long/root/t", localStorage(), "/other/x", "very/long/root/other/x"});

    check({"./ location", "./lineitem_iceberg", "lineitem_iceberg", localStorage(), "./lineitem_iceberg/metadata/snap.avro",
        "lineitem_iceberg/metadata/snap.avro"});
    /// A relative location naming another directory: relative paths are relative to the base of the queried path.
    check({"relative location of a renamed table", "./lineitem_iceberg", "lineitem_iceberg_no_hint", localStorage(),
        "lineitem_iceberg/data/x.parquet", "lineitem_iceberg/data/x.parquet"});
    check({"relative location of another table", "x/other_table", "x/this_table", localStorage(),
        "x/other_table/metadata/m.avro", "x/other_table/metadata/m.avro"});

    check({"another bucket", "s3://bucket/x/t", "x/t", s3Storage(), "s3://other/y/z", std::nullopt});
    check({"more directories outside the location than in the table root", "/a/b/c/t", "t", localStorage(), "/q/x", std::nullopt});
    check({"another storage", "file:///a/t", "a/t", localStorage(), "s3://bucket/x/snap.avro", std::nullopt});
}

/// An object key is a byte string; a filesystem names `a//b` as `a/b`.
TEST(IcebergPathResolver, EmptyComponents)
{
    check({"object storage", "s3://bucket/x/t", "x/t", s3Storage(), "s3://bucket/x//t/data/a", "x//t/data/a"});
    check({"local", "/w/t", "w/t", localStorage(), "/w//t/data/a", "w/t/data/a"});
    check({"hdfs", "hdfs://nn/x/t", "/x/t", hdfsStorage(), "hdfs://nn/x/t/data//a", "/x/t/data//a"});
}

TEST(IcebergPathResolver, StorageIdentity)
{
    IcebergPathResolver s3("s3://bucket/x/t", "x/t", s3Storage(/* allow_foreign_namespaces */ true));
    ASSERT_TRUE(s3.storageIdentity(metadataPath("s3://other/k")).has_value());
    EXPECT_EQ(s3.storageIdentity(metadataPath("s3://other/k")), s3.storageIdentity(metadataPath("s3a://other/k")));
    EXPECT_NE(s3.storageIdentity(metadataPath("s3://other/k")), s3.storageIdentity(metadataPath("s3://bucket/k")));
    EXPECT_NE(s3.storageIdentity(metadataPath("s3://bucket/x//t/data/a")), s3.storageIdentity(metadataPath("s3://bucket/x/t/data/a")));

    IcebergPathResolver local("/t", "t", localStorage());
    ASSERT_TRUE(local.storageIdentity(metadataPath("t//data/a")).has_value());
    EXPECT_EQ(local.storageIdentity(metadataPath("t//data/a")), local.storageIdentity(metadataPath("t/data/a")));

    IcebergPathResolver hdfs("hdfs://nn/x/t", "/x/t", hdfsStorage());
    ASSERT_TRUE(hdfs.storageIdentity(metadataPath("hdfs://nn/x/t/data//a")).has_value());
    EXPECT_EQ(hdfs.storageIdentity(metadataPath("hdfs://nn/x/t/data//a")), hdfs.storageIdentity(metadataPath("hdfs://nn/x/t/data/a")));

    EXPECT_EQ(IcebergPathResolver("s3://bucket/x/t", "x/t", s3Storage()).storageIdentity(metadataPath("s3://other/y")), std::nullopt);
}

TEST(IcebergPathResolver, SameStorageIdentity)
{
    auto identity = [](const IcebergPathResolver & resolver, const String & path) { return resolver.storageIdentity(metadataPath(path)); };
    auto same = [&](const IcebergPathResolver & resolver, const String & lhs, const String & rhs)
    {
        const auto expected = identity(resolver, lhs);
        ASSERT_TRUE(expected.has_value()) << lhs;
        EXPECT_EQ(expected, identity(resolver, rhs)) << rhs;
    };

    IcebergPathResolver impala("/test-warehouse/t", "w/t", localStorage());
    same(impala, "/test-warehouse/t/data/a.parquet", "hdfs://localhost:20500/test-warehouse/t/data/a.parquet");
    EXPECT_NE(identity(impala, "/test-warehouse/t/data/a.parquet"), identity(impala, "/test-warehouse/t/data/b.parquet"));

    IcebergPathResolver local("/tbl", "tbl", localStorage());
    same(local, "/tbl/data/x", "file:///tbl/data/x");
    same(local, "/tbl/data/x", "tbl/data/x");
    same(local, "/tbl/data/x", "/tbl/./data//x");

    IcebergPathResolver foreign("s3://bucket/x/t", "x/t", s3Storage(/* allow_foreign_namespaces */ true));
    EXPECT_NE(identity(foreign, "s3://other/shared/x"), identity(foreign, "s3://bucket/shared/x"));
    same(foreign, "s3a://bucket/x/t/data/a", "s3://bucket/x/t/data/a");

    IcebergPathResolver s3("s3://bucket/x/t", "x/t", s3Storage());
    EXPECT_EQ(s3.storageIdentity(metadataPath("s3://other/y")), std::nullopt);
}

TEST(IcebergPathResolver, MatchPositionDeleteBounds)
{
    IcebergPathResolver resolver("/x", "x", localStorage());
    auto scope = [&](std::optional<String> lower, std::optional<String> upper, const String & data)
    {
        auto to_path = [](const std::optional<String> & bound) -> std::optional<IcebergPathFromMetadata>
        {
            if (!bound)
                return std::nullopt;
            return metadataPath(*bound);
        };
        auto to_key = [&](const std::optional<IcebergPathFromMetadata> & path)
        { return path ? resolver.positionDeleteMatchKey(*path) : PositionDeleteMatchKey{}; };
        const auto lower_path = to_path(lower);
        const auto upper_path = to_path(upper);
        const auto data_path = metadataPath(data);
        return resolver.matchPositionDeleteBounds(
            lower_path, to_key(lower_path), upper_path, to_key(upper_path), data_path, resolver.positionDeleteMatchKey(data_path));
    };

    EXPECT_EQ(scope("/x/data/m", "/x/data/m", "/x/data/m"), PositionDeleteScope::ThisFile);
    EXPECT_EQ(scope("hdfs://h/x/data/m", "hdfs://h/x/data/m", "/x/data/m"), PositionDeleteScope::ThisFile);
    EXPECT_EQ(scope("/x/data/z", "/x/data/z", "/x/data/m"), PositionDeleteScope::None);

    EXPECT_EQ(scope("/x/data/a", "/x/data/z", "/x/data/m"), PositionDeleteScope::Range);
    EXPECT_EQ(scope("/x/data/n", "/x/data/z", "/x/data/m"), PositionDeleteScope::None);
    /// Bounds under other top-level directories prove the range too.
    EXPECT_EQ(scope("/a/x", "/s/z", "/t/data/m"), PositionDeleteScope::None);
    EXPECT_EQ(scope("/a/x", "/z/z", "/t/data/m"), PositionDeleteScope::Range);
    /// Bounds truncated inside a component still prove the range.
    EXPECT_EQ(scope("/tmp/iceberg/dat", "/tmp/iceberg/dau", "/tmp/iceberg/data/a.parquet"), PositionDeleteScope::Range);

    /// The raw range does not prove anything for paths spelled differently.
    EXPECT_EQ(scope("/x/data/a", "/x/data/z", "/x//data/m"), PositionDeleteScope::ThisFile);
    EXPECT_EQ(scope("file:/x/data/a", "file:/x/data/z", "file:///x/data/m"), PositionDeleteScope::ThisFile);
    EXPECT_EQ(scope("s3a://b/x/data/a", "s3a://b/x/data/z", "s3://b/x/data/m"), PositionDeleteScope::ThisFile);
    EXPECT_EQ(scope("hdfs://localhost", "hdfs://localhosu", "/test-warehouse/t/data/a"), PositionDeleteScope::ThisFile);
    EXPECT_EQ(scope("/x/data/", "/x/data/z", "/x/data/m"), PositionDeleteScope::ThisFile);
    /// A lower bound continuing the prefix with a byte below `/` may hold `file:///t/data/m`.
    EXPECT_EQ(scope("file:/!/a", "file:/s/z", "file:/t/data/m"), PositionDeleteScope::Range);
    /// A relative path has no canonical prefix.
    EXPECT_EQ(scope("x/data/n", "x/data/z", "x/data/m"), PositionDeleteScope::ThisFile);
    /// One bound or none proves no range.
    EXPECT_EQ(scope(std::nullopt, std::nullopt, "/x/data/m"), PositionDeleteScope::Range);
    EXPECT_EQ(scope("/x/data/a", std::nullopt, "/x/data/m"), PositionDeleteScope::Range);
    EXPECT_EQ(scope("/x/data/n", std::nullopt, "/x/data/m"), PositionDeleteScope::Range);
}

TEST(IcebergPathResolver, CanonicalPrefix)
{
    using Prefix = std::optional<std::string_view>;
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("/t/data/a"), Prefix("/"));
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("hdfs://nn:8020/t/a"), Prefix("hdfs://nn:8020/"));
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("file:/t/a"), Prefix("file:/"));
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("file:///t/m"), Prefix("file:///"));
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("s3a://b/t/a"), Prefix("s3a://b/"));
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("t/a"), std::nullopt);
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("s3://b"), std::nullopt);
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("/t//a"), std::nullopt);
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("/t/./a"), std::nullopt);
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("/t/../t/a"), std::nullopt);
    EXPECT_EQ(IcebergPathResolver::canonicalPrefix("/t/data/"), std::nullopt);
}
