#include <gtest/gtest.h>

#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <limits>

using namespace DB;
using namespace DB::Iceberg;

TEST(IcebergCountShortcuts, HasEqualityAndPositionDeleteHelpers)
{
    Iceberg::IcebergObjectSerializableInfo info;
    info.data_object_file_path_key = Iceberg::IcebergPathFromMetadata::deserialize("s3://bucket/data/file.parquet");
    info.file_format = "PARQUET";

    auto plain = std::make_shared<ObjectInfo>(RelativePathWithMetadata{"data/file.parquet"});
    EXPECT_FALSE(hasIcebergEqualityDeletes(plain));
    EXPECT_FALSE(hasIcebergPositionDeletes(plain));

    auto iceberg = std::make_shared<IcebergDataObjectInfo>(RelativePathWithMetadata{"data/file.parquet"}, info);
    EXPECT_FALSE(hasIcebergEqualityDeletes(iceberg));
    EXPECT_FALSE(hasIcebergPositionDeletes(iceberg));

    iceberg->info.equality_deletes_objects.push_back(
        Iceberg::EqualityDeleteObject{
            .file_path = "s3://bucket/deletes/eq.parquet",
            .file_format = "PARQUET",
            .equality_ids = std::vector<Int32>{1},
            .schema_id = 0,
        });
    EXPECT_TRUE(hasIcebergEqualityDeletes(iceberg));
    EXPECT_FALSE(hasIcebergPositionDeletes(iceberg));

    iceberg->info.position_deletes_objects.push_back(
        Iceberg::PositionDeleteObject{
            .file_path = "s3://bucket/deletes/pos.parquet",
            .file_format = "PARQUET",
            .reference_data_file_path = std::nullopt,
            .sequence_number = 1,
        });
    EXPECT_TRUE(hasIcebergEqualityDeletes(iceberg));
    EXPECT_TRUE(hasIcebergPositionDeletes(iceberg));
}

TEST(IcebergCountShortcuts, GetTotalRowsRequiresBothSummaryFields)
{
    Iceberg::IcebergDataSnapshot snapshot;
    snapshot.total_rows = 100;
    snapshot.total_position_delete_rows = 0;
    snapshot.total_equality_delete_rows = 0;

    ASSERT_TRUE(snapshot.getTotalRows().has_value());
    EXPECT_EQ(*snapshot.getTotalRows(), 100u);

    snapshot.total_position_delete_rows = 10;
    ASSERT_TRUE(snapshot.getTotalRows().has_value());
    EXPECT_EQ(*snapshot.getTotalRows(), 90u);

    snapshot.total_position_delete_rows = std::nullopt;
    EXPECT_FALSE(snapshot.getTotalRows().has_value());

    snapshot.total_position_delete_rows = 0;
    snapshot.total_rows = std::nullopt;
    EXPECT_FALSE(snapshot.getTotalRows().has_value());
}

/// Snapshot-summary arithmetic can disagree with manifests (poisoned incremental totals,
/// stale total-position-deletes after rewrite). getTotalRows is only for mismatch warnings;
/// IcebergMetadata::totalRows must prefer the manifest record_count sum.
TEST(IcebergCountShortcuts, SummaryArithmeticCanDisagreeWithManifestTruth)
{
    Iceberg::IcebergDataSnapshot snapshot;
    snapshot.total_rows = 999999;
    snapshot.total_position_delete_rows = 0;
    snapshot.total_equality_delete_rows = 0;

    ASSERT_TRUE(snapshot.getTotalRows().has_value());
    EXPECT_EQ(*snapshot.getTotalRows(), 999999u);

    snapshot.total_rows = 90;
    snapshot.total_position_delete_rows = 10;
    ASSERT_TRUE(snapshot.getTotalRows().has_value());
    EXPECT_EQ(*snapshot.getTotalRows(), 80u);
}

/// Live deletion vectors: summary subtraction may look plausible while DVs are live.
/// totalRows must open manifests, see POSITION_DELETE files, and fail closed to a scan.
TEST(IcebergCountShortcuts, LiveDeletionVectorSummaryLooksPlausible)
{
    Iceberg::IcebergDataSnapshot snapshot;
    snapshot.total_rows = 100;
    snapshot.total_position_delete_rows = 10;
    snapshot.total_equality_delete_rows = 0;

    ASSERT_TRUE(snapshot.getTotalRows().has_value());
    EXPECT_EQ(*snapshot.getTotalRows(), 90u);
}

TEST(IcebergCountShortcuts, GetTotalRowsFailsClosedWhenPositionDeletesExceedRows)
{
    Iceberg::IcebergDataSnapshot snapshot;
    snapshot.total_rows = 5;
    snapshot.total_position_delete_rows = 6;
    EXPECT_FALSE(snapshot.getTotalRows().has_value());
}

namespace
{

ProcessedManifestFileEntryPtr makeDataEntryForRecordCount(
    Int64 record_count,
    std::unordered_map<Int32, ColumnInfo> columns_infos = {})
{
    auto parsed = std::make_shared<ParsedManifestFileEntry>(
        FileContentType::DATA,
        IcebergPathFromMetadata::deserialize("s3://bucket/data/file.parquet"),
        /*row_number=*/0,
        ManifestEntryStatus::ADDED,
        /*written_sequence_number=*/std::nullopt,
        /*written_snapshot_id=*/std::nullopt,
        DB::Row{},
        std::move(columns_infos),
        std::unordered_map<Int32, std::pair<Field, Field>>{},
        /*file_format=*/"PARQUET",
        /*lower_reference_data_file_path=*/std::nullopt,
        /*upper_reference_data_file_path=*/std::nullopt,
        /*equality_ids=*/std::nullopt,
        /*sort_order_id=*/std::nullopt,
        record_count,
        /*file_size_in_bytes=*/100);

    auto processed = std::make_shared<ProcessedManifestFileEntry>();
    processed->parsed_entry = std::move(parsed);
    processed->common_partition_specification = std::make_shared<PartitionSpecification>();
    processed->sequence_number = 0;
    processed->resolved_schema_id = 0;
    processed->manifest_file_path = "s3://bucket/metadata/manifest.avro";
    return processed;
}

}

TEST(IcebergRecordCountAggregate, SumsRecordCountIgnoringValueCounts)
{
    ColumnInfo nested_list_stats;
    nested_list_stats.rows_count = 1000; /// nested element count, not row count

    const auto total = getRecordCountInAllFilesExcludingDeleted({
        makeDataEntryForRecordCount(/*record_count=*/10, {{/*column_id=*/2, nested_list_stats}}),
        makeDataEntryForRecordCount(/*record_count=*/5),
    });

    ASSERT_TRUE(total.has_value());
    EXPECT_EQ(*total, 15);
}

TEST(IcebergRecordCountAggregate, SucceedsWithoutValueCounts)
{
    const auto total = getRecordCountInAllFilesExcludingDeleted({
        makeDataEntryForRecordCount(/*record_count=*/42),
    });

    ASSERT_TRUE(total.has_value());
    EXPECT_EQ(*total, 42);
}

TEST(IcebergRecordCountAggregate, EmptyManifestIsZero)
{
    const auto total = getRecordCountInAllFilesExcludingDeleted({});
    ASSERT_TRUE(total.has_value());
    EXPECT_EQ(*total, 0);
}

TEST(IcebergRecordCountAggregate, NegativeRecordCountFailsClosed)
{
    const auto total = getRecordCountInAllFilesExcludingDeleted({
        makeDataEntryForRecordCount(/*record_count=*/10),
        makeDataEntryForRecordCount(/*record_count=*/-1),
    });
    EXPECT_FALSE(total.has_value());
}

TEST(IcebergRecordCountAggregate, OverflowFailsClosed)
{
    const auto total = getRecordCountInAllFilesExcludingDeleted({
        makeDataEntryForRecordCount(/*record_count=*/std::numeric_limits<Int64>::max()),
        makeDataEntryForRecordCount(/*record_count=*/1),
    });
    EXPECT_FALSE(total.has_value());
}

namespace
{

ColumnInfo columnBytes(Int64 bytes)
{
    ColumnInfo info;
    info.bytes_size = bytes;
    return info;
}

}

TEST(IcebergBytesSizeAggregate, SumsFirstDeclaredColumnBytesSize)
{
    const auto total = getBytesSizeInAllDataFilesExcludingDeleted({
        makeDataEntryForRecordCount(/*record_count=*/1, {{/*column_id=*/1, columnBytes(10)}}),
        makeDataEntryForRecordCount(/*record_count=*/1, {{/*column_id=*/1, columnBytes(5)}}),
    });

    ASSERT_TRUE(total.has_value());
    EXPECT_EQ(*total, 15);
}

TEST(IcebergBytesSizeAggregate, MissingBytesSizeFailsClosed)
{
    const auto total = getBytesSizeInAllDataFilesExcludingDeleted({
        makeDataEntryForRecordCount(/*record_count=*/1, {{/*column_id=*/1, columnBytes(10)}}),
        makeDataEntryForRecordCount(/*record_count=*/1),
    });
    EXPECT_FALSE(total.has_value());
}

TEST(IcebergBytesSizeAggregate, NegativeBytesSizeFailsClosed)
{
    const auto total = getBytesSizeInAllDataFilesExcludingDeleted({
        makeDataEntryForRecordCount(/*record_count=*/1, {{/*column_id=*/1, columnBytes(-1)}}),
    });
    EXPECT_FALSE(total.has_value());
}

TEST(IcebergBytesSizeAggregate, OverflowFailsClosed)
{
    const auto total = getBytesSizeInAllDataFilesExcludingDeleted({
        makeDataEntryForRecordCount(
            /*record_count=*/1, {{/*column_id=*/1, columnBytes(std::numeric_limits<Int64>::max())}}),
        makeDataEntryForRecordCount(/*record_count=*/1, {{/*column_id=*/1, columnBytes(1)}}),
    });
    EXPECT_FALSE(total.has_value());
}

#endif
