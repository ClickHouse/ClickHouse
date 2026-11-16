#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

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

/// `reverseResolve` returns the spelling `resolve` maps back to the same storage path. A table at a
/// bucket root has an empty `table_root`, so its storage paths carry no leading separator.
TEST(IcebergPathResolver, ReverseResolve)
{
    const Iceberg::BlobStorageDescription s3{.type_name = "s3", .namespace_name = "bkt", .allow_foreign_namespaces = false};
    const Iceberg::BlobStorageDescription local{.type_name = "local", .namespace_name = "", .allow_foreign_namespaces = false};
    struct Case
    {
        String table_location;
        String table_root;
        Iceberg::BlobStorageDescription blob_storage;
        String storage_path;
        String expected;
    };
    for (const auto & test_case : {
             Case{"s3://bkt/", "", s3, "metadata/v1.metadata.json", "s3://bkt/metadata/v1.metadata.json"},
             Case{"s3://bkt/tbl", "tbl", s3, "tbl/metadata/v1.metadata.json", "s3://bkt/tbl/metadata/v1.metadata.json"},
             Case{"/tbl", "tbl", s3, "tbl/metadata/v1.metadata.json", "/tbl/metadata/v1.metadata.json"},
             Case{"/abs/tbl", "/abs/tbl", local, "/abs/tbl/metadata/v1.metadata.json", "/abs/tbl/metadata/v1.metadata.json"}})
    {
        IcebergPathResolver resolver(test_case.table_location, test_case.table_root, test_case.blob_storage);
        auto metadata_path = resolver.reverseResolve(test_case.storage_path);
        EXPECT_EQ(metadata_path.serialize(), test_case.expected) << test_case.table_location;
        EXPECT_EQ(resolver.resolve(metadata_path), test_case.storage_path) << test_case.table_location;
    }
}
