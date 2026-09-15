#include <gtest/gtest.h>

#include <config.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

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

namespace
{

std::shared_ptr<ParsedManifestFileEntry> makePositionDeleteEntry(
    String file_format, std::optional<Int64> content_offset, std::optional<Int64> content_size_in_bytes)
{
    return std::make_shared<ParsedManifestFileEntry>(
        FileContentType::POSITION_DELETE,
        IcebergPathFromMetadata::deserialize("s3://bucket/deletes/dv.bin"),
        /*row_number=*/0,
        ManifestEntryStatus::ADDED,
        /*written_sequence_number=*/std::nullopt,
        /*written_file_sequence_number=*/std::nullopt,
        /*written_snapshot_id=*/std::nullopt,
        DB::Row{},
        std::unordered_map<Int32, ColumnInfo>{},
        std::unordered_map<Int32, std::pair<Field, Field>>{},
        std::move(file_format),
        IcebergPathFromMetadata::deserialize("s3://bucket/data/file.parquet"),
        IcebergPathFromMetadata::deserialize("s3://bucket/data/file.parquet"),
        /*equality_ids=*/std::nullopt,
        /*sort_order_id=*/std::nullopt,
        /*record_count=*/2,
        /*file_size_in_bytes=*/45,
        content_offset,
        content_size_in_bytes);
}

}

TEST(IcebergDeletionVectorClassification, OffsetsClassifyAsDeletionVectorRegardlessOfFileFormat)
{
    EXPECT_TRUE(makePositionDeleteEntry("puffin", 1, 44)->isDeletionVector());
    EXPECT_TRUE(makePositionDeleteEntry("PUFFIN", 4, 44)->isDeletionVector());
    EXPECT_TRUE(makePositionDeleteEntry("parquet", 1, 44)->isDeletionVector());
    EXPECT_FALSE(makePositionDeleteEntry("puffin", std::nullopt, std::nullopt)->isDeletionVector());
    EXPECT_FALSE(makePositionDeleteEntry("parquet", std::nullopt, std::nullopt)->isDeletionVector());
    EXPECT_FALSE(makePositionDeleteEntry("parquet", 1, std::nullopt)->isDeletionVector());
}

#endif
