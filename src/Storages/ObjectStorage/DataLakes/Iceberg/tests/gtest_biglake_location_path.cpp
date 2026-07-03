/// Tests for the Iceberg metadata location-path selection introduced alongside
/// BigLake support (commit 554efb08cc1 and related).
///
/// The `IcebergMetadata::createInitial` function selects the value written to
/// the `"location"` field of the v1.metadata.json file via three branches:
///
///   1. BigLake catalog   → `configuration->getRawURI()` (canonical `gs://…`)
///   2. No catalog, `write_full_path_in_iceberg_metadata = false`
///                        → `getRawPath().path`, prefixed with "/" when the
///                          path contains no "://"
///   3. No catalog, `write_full_path_in_iceberg_metadata = true`
///                        → `getTypeName() + "://" + getNamespace() + "/" + getRawPath().path`
///
/// `createEmptyMetadataFile` then embeds the chosen `location_path` string
/// directly into the metadata JSON object.  These tests verify:
///
///   a. `createEmptyMetadataFile` faithfully stores any `path_location` string
///      in the JSON `"location"` key.
///   b. The three path-normalisation rules above produce the expected strings
///      for representative inputs.

#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ColumnsDescription.h>

namespace
{

/// Call `createEmptyMetadataFile` with no columns, no partition, no sort order,
/// and return the `"location"` value from the resulting JSON object.
std::string locationFromMetadata(const std::string & path_location)
{
    DB::ColumnsDescription columns;
    auto [json_obj, /*json_str*/] = DB::Iceberg::createEmptyMetadataFile(
        path_location,
        columns,
        /* partition_by */ nullptr,
        /* order_by */ nullptr,
        /* context */ nullptr);
    return json_obj->getValue<std::string>(DB::Iceberg::f_location);
}

// ---------------------------------------------------------------------------
// Helper: replicate the three normalisation branches from `createInitial`
// so each rule can be unit-tested independently of a live object storage.
// ---------------------------------------------------------------------------

/// Branch 1 (BigLake): use the raw URI unchanged.
std::string bigLakeLocationPath(const std::string & raw_uri)
{
    return raw_uri;
}

/// Branch 2 (non-BigLake, write_full_path = false):
///   - preserve path if it already contains "://"
///   - preserve path if it starts with '/'
///   - otherwise prefix with '/'
std::string standardLocationPath(const std::string & raw_path)
{
    std::string location_path = raw_path;
    if (location_path.find("://") == std::string::npos && !location_path.starts_with('/'))
        location_path = "/" + location_path;
    return location_path;
}

/// Branch 3 (non-BigLake, write_full_path = true):
///   `type + "://" + namespace_ + "/" + path`
std::string fullLocationPath(
    const std::string & type_name,
    const std::string & namespace_,
    const std::string & raw_path)
{
    return type_name + "://" + namespace_ + "/" + raw_path;
}

} // namespace

// ---------------------------------------------------------------------------
// Part A – `createEmptyMetadataFile` stores `path_location` verbatim
// ---------------------------------------------------------------------------

TEST(BigLakeLocationPath, CreateEmptyMetadataFilePreservesGsUri)
{
    const std::string gs_uri = "gs://my-bucket/datasets/my_table/";
    EXPECT_EQ(locationFromMetadata(gs_uri), gs_uri);
}

TEST(BigLakeLocationPath, CreateEmptyMetadataFilePreservesAbsolutePath)
{
    const std::string abs_path = "/bucket/path/to/table/";
    EXPECT_EQ(locationFromMetadata(abs_path), abs_path);
}

TEST(BigLakeLocationPath, CreateEmptyMetadataFilePreservesS3Uri)
{
    const std::string s3_uri = "s3://my-bucket/path/to/table/";
    EXPECT_EQ(locationFromMetadata(s3_uri), s3_uri);
}

TEST(BigLakeLocationPath, CreateEmptyMetadataFilePreservesRelativePath)
{
    // Relative paths are stored as-is; the caller is responsible for prepending '/'.
    const std::string rel_path = "path/to/table/";
    EXPECT_EQ(locationFromMetadata(rel_path), rel_path);
}

// ---------------------------------------------------------------------------
// Part B – path-normalisation rules (branches from `createInitial`)
// ---------------------------------------------------------------------------

// Branch 1 (BigLake): `gs://…` URI is used verbatim.
TEST(BigLakeLocationPath, BigLakeBranchUsesRawUri)
{
    EXPECT_EQ(bigLakeLocationPath("gs://bucket/datasets/tbl/"), "gs://bucket/datasets/tbl/");
}

TEST(BigLakeLocationPath, BigLakeBranchDoesNotModifyUri)
{
    // Even a path that would otherwise get a "/" prefix is left alone.
    EXPECT_EQ(bigLakeLocationPath("gs://bucket/no-trailing-slash"), "gs://bucket/no-trailing-slash");
}

// Branch 2a: path already starts with '/' — kept unchanged.
TEST(BigLakeLocationPath, StandardBranchPreservesAbsolutePath)
{
    EXPECT_EQ(standardLocationPath("/bucket/path/to/table/"), "/bucket/path/to/table/");
}

// Branch 2b: path already contains "://" — kept unchanged (no "/" prefix added).
TEST(BigLakeLocationPath, StandardBranchPreservesUriWithScheme)
{
    EXPECT_EQ(standardLocationPath("s3://bucket/path/"), "s3://bucket/path/");
}

// Branch 2c: relative path without "://" and without leading "/" — gets "/" prefix.
TEST(BigLakeLocationPath, StandardBranchAddsSlashPrefixToRelativePath)
{
    EXPECT_EQ(standardLocationPath("bucket/path/to/table/"), "/bucket/path/to/table/");
}

TEST(BigLakeLocationPath, StandardBranchAddsSlashPrefixToSingleSegment)
{
    EXPECT_EQ(standardLocationPath("mytable"), "/mytable");
}

// Branch 3 (`write_full_path_in_iceberg_metadata = true`): assembles full URI.
TEST(BigLakeLocationPath, FullPathBranchProducesCorrectUri)
{
    EXPECT_EQ(
        fullLocationPath("s3", "my-bucket", "path/to/table/"),
        "s3://my-bucket/path/to/table/");
}

TEST(BigLakeLocationPath, FullPathBranchWorksForGcsType)
{
    EXPECT_EQ(
        fullLocationPath("gs", "gcs-bucket", "datasets/tbl/"),
        "gs://gcs-bucket/datasets/tbl/");
}

// ---------------------------------------------------------------------------
// Part C – end-to-end: normalised path → JSON `"location"` field
// ---------------------------------------------------------------------------

/// BigLake branch: raw URI ends up in the metadata JSON unchanged.
TEST(BigLakeLocationPath, BigLakeUriPreservedInMetadataJson)
{
    const std::string raw_uri = "gs://my-bucket/iceberg_tables/orders/";
    const std::string location_path = bigLakeLocationPath(raw_uri);
    EXPECT_EQ(locationFromMetadata(location_path), raw_uri);
}

/// Standard branch (write_full_path=false), relative path: metadata JSON
/// receives the "/" prefixed path.
TEST(BigLakeLocationPath, RelativePathPrefixedInMetadataJson)
{
    const std::string raw_path = "iceberg_tables/orders/";
    const std::string location_path = standardLocationPath(raw_path);
    EXPECT_EQ(locationFromMetadata(location_path), "/iceberg_tables/orders/");
}

/// Standard branch (write_full_path=true): metadata JSON receives the full URI.
TEST(BigLakeLocationPath, FullUriWrittenToMetadataJson)
{
    const std::string location_path = fullLocationPath("s3", "my-bucket", "iceberg_tables/orders/");
    EXPECT_EQ(locationFromMetadata(location_path), "s3://my-bucket/iceberg_tables/orders/");
}

#endif // USE_AVRO
