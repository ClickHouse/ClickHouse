#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

using namespace DB::Iceberg;

/// In antalya-26.3 the path resolution lived in the free function
/// getProperFilePathFromMetadataInfo(data_path, common_path, table_location).
/// In 26.6 the same logic is provided by IcebergPathResolver, where
/// common_path became table_root and the data path is passed as an
/// IcebergPathFromMetadata. The mapping is:
///   getProperFilePathFromMetadataInfo(data_path, common_path, table_location)
///     == IcebergPathResolver(table_location, common_path).resolve(deserialize(data_path))
static String resolvePath(std::string_view data_path, std::string_view common_path, std::string_view table_location)
{
    IcebergPathResolver resolver(String(table_location), String(common_path));
    return resolver.resolve(IcebergPathFromMetadata::deserialize(String(data_path)));
}

TEST(GetProperFilePathFromMetadataInfo, S3SchemePreservesPercentEncodedSlash)
{
    auto result = resolvePath(
        "s3://bucket/warehouse/data/partition=us%2Fwest/file.parquet",
        "warehouse",
        "s3://bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/partition=us%2Fwest/file.parquet");
}

TEST(GetProperFilePathFromMetadataInfo, SimpleKeyWithoutEncoding)
{
    auto result = resolvePath(
        "s3://bucket/warehouse/data/file.parquet",
        "warehouse",
        "s3://bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/file.parquet");
}

TEST(GetProperFilePathFromMetadataInfo, MultiplePercentEncodedSegments)
{
    auto result = resolvePath(
        "s3://bucket/warehouse/data/region=us%2Fwest/city=san%20francisco/file.parquet",
        "warehouse",
        "s3://bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/region=us%2Fwest/city=san%20francisco/file.parquet");
}

TEST(GetProperFilePathFromMetadataInfo, HttpSchemePreservesPercentEncodedSlash)
{
    auto result = resolvePath(
        "http://minio:9000/bucket/warehouse/data/partition=us%2Fwest/file.parquet",
        "warehouse",
        "http://minio:9000/bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/partition=us%2Fwest/file.parquet");
}
