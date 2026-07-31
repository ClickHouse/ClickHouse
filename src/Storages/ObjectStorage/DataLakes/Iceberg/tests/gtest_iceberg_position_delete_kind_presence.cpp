#include <gtest/gtest.h>

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

using namespace DB;
using namespace DB::Iceberg;

namespace
{

ProcessedManifestFileEntryPtr makePositionDeleteEntry(
    const String & file_format,
    std::optional<Int64> content_offset = std::nullopt,
    std::optional<Int64> content_size_in_bytes = std::nullopt)
{
    auto parsed = std::make_shared<ParsedManifestFileEntry>(
        FileContentType::POSITION_DELETE,
        IcebergPathFromMetadata::deserialize("s3://bucket/deletes/file"),
        /*row_number=*/0,
        ManifestEntryStatus::ADDED,
        /*written_sequence_number=*/std::nullopt,
        /*written_snapshot_id=*/std::nullopt,
        DB::Row{},
        std::unordered_map<Int32, ColumnInfo>{},
        std::unordered_map<Int32, std::pair<Field, Field>>{},
        file_format,
        /*lower_reference_data_file_path=*/std::nullopt,
        /*upper_reference_data_file_path=*/std::nullopt,
        /*equality_ids=*/std::nullopt,
        /*sort_order_id=*/std::nullopt,
        /*record_count=*/1,
        /*file_size_in_bytes=*/100,
        content_offset,
        content_size_in_bytes);

    auto processed = std::make_shared<ProcessedManifestFileEntry>();
    processed->parsed_entry = std::move(parsed);
    processed->common_partition_specification = std::make_shared<PartitionSpecification>();
    processed->sequence_number = 0;
    processed->resolved_schema_id = 0;
    processed->manifest_file_path = "s3://bucket/metadata/manifest.avro";
    return processed;
}

}

TEST(IcebergPositionDeleteKindPresence, Empty)
{
    const auto presence = getPositionDeleteKindPresence({});
    EXPECT_FALSE(presence.has_deletion_vectors);
    EXPECT_FALSE(presence.has_parquet_position_deletes);
    EXPECT_FALSE(presence.hasBoth());
}

TEST(IcebergPositionDeleteKindPresence, DeletionVectorsOnly)
{
    const auto presence = getPositionDeleteKindPresence({
        makePositionDeleteEntry("puffin", /*content_offset=*/4, /*content_size_in_bytes=*/40),
        makePositionDeleteEntry("PUFFIN", /*content_offset=*/50, /*content_size_in_bytes=*/40),
    });
    EXPECT_TRUE(presence.has_deletion_vectors);
    EXPECT_FALSE(presence.has_parquet_position_deletes);
    EXPECT_FALSE(presence.hasBoth());
}

TEST(IcebergPositionDeleteKindPresence, ParquetPositionDeletesOnly)
{
    const auto presence = getPositionDeleteKindPresence({
        makePositionDeleteEntry("parquet"),
        makePositionDeleteEntry("puffin"), /// puffin without content offset/size is not a DV
    });
    EXPECT_FALSE(presence.has_deletion_vectors);
    EXPECT_TRUE(presence.has_parquet_position_deletes);
    EXPECT_FALSE(presence.hasBoth());
}

TEST(IcebergPositionDeleteKindPresence, CoexistenceFailsClosedGate)
{
    const auto presence = getPositionDeleteKindPresence({
        makePositionDeleteEntry("puffin", /*content_offset=*/4, /*content_size_in_bytes=*/40),
        makePositionDeleteEntry("parquet"),
    });
    EXPECT_TRUE(presence.has_deletion_vectors);
    EXPECT_TRUE(presence.has_parquet_position_deletes);
    EXPECT_TRUE(presence.hasBoth());
}

TEST(IcebergPositionDeleteKindPresence, CrossManifestAggregation)
{
    /// Mimic totalRows: OR presence across manifests before deciding fail-closed.
    auto first = getPositionDeleteKindPresence({
        makePositionDeleteEntry("puffin", /*content_offset=*/4, /*content_size_in_bytes=*/40),
    });
    auto second = getPositionDeleteKindPresence({
        makePositionDeleteEntry("parquet"),
    });

    const bool has_deletion_vectors = first.has_deletion_vectors || second.has_deletion_vectors;
    const bool has_parquet_position_deletes = first.has_parquet_position_deletes || second.has_parquet_position_deletes;
    EXPECT_TRUE(has_deletion_vectors && has_parquet_position_deletes);
}

#endif
