#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB::Iceberg;

TEST(GetProperFilePathFromMetadataInfo, S3SchemePreservesPercentEncodedSlash)
{
    auto result = getProperFilePathFromMetadataInfo(
        "s3://bucket/warehouse/data/partition=us%2Fwest/file.parquet",
        "warehouse",
        "s3://bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/partition=us%2Fwest/file.parquet");
}

TEST(GetProperFilePathFromMetadataInfo, SimpleKeyWithoutEncoding)
{
    auto result = getProperFilePathFromMetadataInfo(
        "s3://bucket/warehouse/data/file.parquet",
        "warehouse",
        "s3://bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/file.parquet");
}

TEST(GetProperFilePathFromMetadataInfo, MultiplePercentEncodedSegments)
{
    auto result = getProperFilePathFromMetadataInfo(
        "s3://bucket/warehouse/data/region=us%2Fwest/city=san%20francisco/file.parquet",
        "warehouse",
        "s3://bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/region=us%2Fwest/city=san%20francisco/file.parquet");
}

TEST(GetProperFilePathFromMetadataInfo, HttpSchemePreservesPercentEncodedSlash)
{
    auto result = getProperFilePathFromMetadataInfo(
        "http://minio:9000/bucket/warehouse/data/partition=us%2Fwest/file.parquet",
        "warehouse",
        "http://minio:9000/bucket/warehouse");
    ASSERT_EQ(result, "warehouse/data/partition=us%2Fwest/file.parquet");
}
