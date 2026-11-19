#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Common/Exception.h>
#include <tuple>

using namespace DB;
using Iceberg::IcebergPathResolver;

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

/// The native GCS object storage reads every key from its own bucket. A path in another bucket must
/// not be reduced to its key, which would read the object with the same key in the table's bucket.
TEST(IcebergPathResolver, NativeGCSRejectsForeignBucket)
{
    IcebergPathResolver resolver(
        "gs://bucket-a/table",
        "table",
        Iceberg::BlobStorageDescription{
            .type_name = "gcs", .namespace_name = "bucket-a", .allow_foreign_namespaces = false, .reject_foreign_namespaces = true});

    using Iceberg::IcebergPathFromMetadata;
    EXPECT_EQ(resolver.resolve(IcebergPathFromMetadata::deserialize("gs://bucket-a/table/data/a.parquet")), "table/data/a.parquet");
    EXPECT_EQ(resolver.resolve(IcebergPathFromMetadata::deserialize("gs://bucket-a/other/table/data/a.parquet")), "table/data/a.parquet");
    EXPECT_THROW(std::ignore = resolver.resolve(IcebergPathFromMetadata::deserialize("gs://bucket-b/table/data/a.parquet")), DB::Exception);
    EXPECT_THROW(std::ignore = resolver.resolve(IcebergPathFromMetadata::deserialize("s3://bucket-b/table/data/a.parquet")), DB::Exception);
}

/// Iceberg implementations resolve Google Cloud Storage locations by the `gs` scheme, not by the
/// `gcs` token of the native backend.
TEST(IcebergLocationURI, NativeGCSUsesGsScheme)
{
    EXPECT_EQ(Iceberg::makeIcebergLocationURI("gcs", "bucket", "table/metadata/v1.metadata.json"),
        "gs://bucket/table/metadata/v1.metadata.json");
}

TEST(IcebergPathResolver, StorageIdentity)
{
    auto identity = [](const IcebergPathResolver & resolver, const String & path)
    { return resolver.storageIdentity(Iceberg::IcebergPathFromMetadata::deserialize(path)); };

    IcebergPathResolver s3("s3://bucket/tbl", "tbl", {.type_name = "s3", .namespace_name = "bucket", .allow_foreign_namespaces = true});
    EXPECT_EQ(identity(s3, "s3://bucket/tbl/data/x.parquet"), "bucket/tbl/data/x.parquet");
    EXPECT_EQ(identity(s3, "s3a://bucket/tbl/data/x.parquet"), "bucket/tbl/data/x.parquet");
    EXPECT_EQ(identity(s3, "s3://other/tbl/x.parquet"), "other/tbl/x.parquet");
    EXPECT_EQ(identity(s3, "s3a://other/tbl/x.parquet"), "other/tbl/x.parquet");

    IcebergPathResolver azure("abfss://c@a.dfs.core.windows.net", "", {.type_name = "azure", .namespace_name = "c", .allow_foreign_namespaces = false});
    EXPECT_EQ(identity(azure, "abfss://c@a.dfs.core.windows.net/foo://bar/x"), "c/foo://bar/x");

    IcebergPathResolver local("/w/t", "/w/t", {.type_name = "local", .namespace_name = "", .allow_foreign_namespaces = false});
    for (const auto * path : {"/w/t/metadata/s.avro", "file:///w/t/metadata/s.avro", "/w/t/metadata//s.avro", "/w/t/./metadata/s.avro"})
        EXPECT_EQ(identity(local, path), "/w/t/metadata/s.avro") << path;

    IcebergPathResolver hdfs("hdfs://h:9000/w/t", "/w/t", {.type_name = "hdfs", .namespace_name = "h:9000", .allow_foreign_namespaces = false});
    for (const auto * path : {"hdfs://h:9000/w/t/data/x.parquet", "hdfs://h:9000/w/t/data//x.parquet", "hdfs://h:9000/w/t/./data/x.parquet"})
        EXPECT_EQ(identity(hdfs, path), "/w/t/data/x.parquet") << path;
}
