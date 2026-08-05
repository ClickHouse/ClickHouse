#include <gtest/gtest.h>

#include <config.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

using namespace DB;
using namespace DB::Iceberg;

namespace DB::ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

TEST(PuffinDeletionVectorReferencedDataFile, AcceptsNonEmptyDirectField)
{
    const auto path = IcebergPathFromMetadata::deserialize("/data/file.parquet");
    const auto manifest = IcebergPathFromMetadata::deserialize("/meta/manifest.avro");
    EXPECT_NO_THROW(requireDirectReferencedDataFileForPuffinDeletionVector(/*set_from_referenced_data_file_field=*/true, path, manifest));
}

TEST(PuffinDeletionVectorReferencedDataFile, RejectsBoundsOnlyFallback)
{
    const auto path = IcebergPathFromMetadata::deserialize("/data/file.parquet");
    const auto manifest = IcebergPathFromMetadata::deserialize("/meta/manifest.avro");
    try
    {
        requireDirectReferencedDataFileForPuffinDeletionVector(/*set_from_referenced_data_file_field=*/false, path, manifest);
        FAIL() << "Expected ICEBERG_SPECIFICATION_VIOLATION";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
        EXPECT_TRUE(e.message().find("referenced_data_file") != std::string::npos);
    }
}

TEST(PuffinDeletionVectorReferencedDataFile, RejectsMissingPath)
{
    const auto manifest = IcebergPathFromMetadata::deserialize("/meta/manifest.avro");
    try
    {
        requireDirectReferencedDataFileForPuffinDeletionVector(/*set_from_referenced_data_file_field=*/true, std::nullopt, manifest);
        FAIL() << "Expected ICEBERG_SPECIFICATION_VIOLATION";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
    }
}

TEST(PuffinDeletionVectorReferencedDataFile, RejectsEmptyPath)
{
    const auto empty_path = IcebergPathFromMetadata::deserialize("");
    const auto manifest = IcebergPathFromMetadata::deserialize("/meta/manifest.avro");
    try
    {
        requireDirectReferencedDataFileForPuffinDeletionVector(/*set_from_referenced_data_file_field=*/true, empty_path, manifest);
        FAIL() << "Expected ICEBERG_SPECIFICATION_VIOLATION";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
    }
}

#endif
