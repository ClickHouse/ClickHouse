#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace
{
using SnapshotSummary = DB::Iceberg::SnapshotSummary;
using Operation = DB::Iceberg::SnapshotSummaryOperation;
}

TEST(IcebergSnapshotSummary, AppendFirstSnapshotTotalsEqualDeltas)
{
    /// Regression: when a fresh table has no parent snapshot, the totals must
    /// equal the deltas, not zero and not 2 x deltas.
    SnapshotSummary summary(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 2,
        .added_records = 3,
        .added_files_size = 1638,
        .num_partitions = 2});
    summary.applyTotals(std::nullopt);

    EXPECT_EQ(summary.totals.records, 3);
    EXPECT_EQ(summary.totals.data_files, 2);
    EXPECT_EQ(summary.totals.files_size, 1638);
    EXPECT_EQ(summary.totals.delete_files, 0);
    EXPECT_EQ(summary.totals.position_deletes, 0);
    EXPECT_EQ(summary.totals.equality_deletes, 0);
}

TEST(IcebergSnapshotSummary, AppendAccumulatesOnParent)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 2, .added_records = 3, .added_files_size = 1638, .num_partitions = 2});
    parent.applyTotals(std::nullopt);

    SnapshotSummary next(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 1,
        .added_records = 2,
        .added_files_size = 823,
        .num_partitions = 1});
    next.applyTotals(parent.totals);

    EXPECT_EQ(next.totals.records, 5);
    EXPECT_EQ(next.totals.data_files, 3);
    EXPECT_EQ(next.totals.files_size, 2461);
}

TEST(IcebergSnapshotSummary, DeleteSubtractsFromParent)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 3, .added_records = 5, .added_files_size = 2461, .num_partitions = 3});
    parent.applyTotals(std::nullopt);

    SnapshotSummary del(DB::Iceberg::SnapshotSummaryUpdateDelete{
        .removed_data_files = 1,
        .removed_records = 2,
        .removed_files_size = 823,
        .removed_position_delete_files = 0,
        .removed_position_deletes = 0,
        .num_partitions = 1});
    del.applyTotals(parent.totals);

    EXPECT_EQ(del.totals.records, 3);
    EXPECT_EQ(del.totals.data_files, 2);
    EXPECT_EQ(del.totals.files_size, 1638);
}

TEST(IcebergSnapshotSummary, OverwriteAddsDeleteCounts)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 3, .added_records = 5, .added_files_size = 2461, .num_partitions = 3});
    parent.applyTotals(std::nullopt);

    SnapshotSummary ow(DB::Iceberg::SnapshotSummaryUpdateOverwrite{
        .added_delete_files = 1,
        .added_files_size = 100,
        .num_partitions = 1,
        .num_deleted_rows = 4});
    ow.applyTotals(parent.totals);

    EXPECT_EQ(ow.totals.delete_files, 1);
    EXPECT_EQ(ow.totals.position_deletes, 4);
    EXPECT_EQ(ow.totals.data_files, 3);  // unchanged: overwrite doesn't touch data file count
    EXPECT_EQ(ow.totals.records, 5);     // unchanged: overwrite doesn't add data records
    EXPECT_EQ(ow.totals.files_size, 2561);
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, DeleteWithoutParentThrows)
{
    SnapshotSummary del(DB::Iceberg::SnapshotSummaryUpdateDelete{
        .removed_data_files = 1,
        .removed_records = 1,
        .removed_files_size = 100,
        .removed_position_delete_files = 0,
        .removed_position_deletes = 0,
        .num_partitions = 1});
    EXPECT_THROW(del.applyTotals(std::nullopt), DB::Exception);
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, OverwriteWithoutParentThrows)
{
    SnapshotSummary ow(DB::Iceberg::SnapshotSummaryUpdateOverwrite{
        .added_delete_files = 1,
        .added_files_size = 100,
        .num_partitions = 1,
        .num_deleted_rows = 1});
    EXPECT_THROW(ow.applyTotals(std::nullopt), DB::Exception);
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, DoubleFinalizeThrows)
{
    SnapshotSummary summary(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 1,
        .added_records = 1,
        .added_files_size = 100,
        .num_partitions = 1});
    summary.applyTotals(std::nullopt);
    EXPECT_THROW(summary.finalize(std::nullopt), DB::Exception);
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, ToJSONBeforeFinalizeThrows)
{
    SnapshotSummary summary;
    EXPECT_THROW((void)summary.toJSON(), DB::Exception);
}
#endif

TEST(IcebergSnapshotSummary, ToJSONAppendFields)
{
    SnapshotSummary summary(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 2, .added_records = 3, .added_files_size = 1638, .num_partitions = 2});
    summary.applyTotals(std::nullopt);

    auto obj = summary.toJSON();
    ASSERT_TRUE(obj);
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_operation), DB::Iceberg::f_append);
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_data_files), "2");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_records), "3");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_files_size), "1638");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_records), "3");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_data_files), "2");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_files_size), "1638");
    EXPECT_FALSE(obj->has(DB::Iceberg::f_removed_data_files));
}

TEST(IcebergSnapshotSummary, ToJSONDeleteFields)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 3, .added_records = 5, .added_files_size = 2461, .num_partitions = 3});
    parent.applyTotals(std::nullopt);

    SnapshotSummary del(DB::Iceberg::SnapshotSummaryUpdateDelete{
        .removed_data_files = 1,
        .removed_records = 2,
        .removed_files_size = 823,
        .removed_position_delete_files = 0,
        .removed_position_deletes = 0,
        .num_partitions = 1});
    del.applyTotals(parent.totals);

    auto obj = del.toJSON();
    ASSERT_TRUE(obj);
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_operation), DB::Iceberg::f_delete);
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_data_files), "1");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_deleted_data_files), "1");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_deleted_records), "2");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_files_size), "823");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_records), "3");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_data_files), "2");
    EXPECT_FALSE(obj->has(DB::Iceberg::f_added_data_files));
}

TEST(IcebergSnapshotSummary, RoundTripThroughJSON)
{
    SnapshotSummary original(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 2, .added_records = 3, .added_files_size = 1638, .num_partitions = 2});
    original.applyTotals(std::nullopt);

    auto obj = original.toJSON();
    auto parsed = SnapshotSummary::fromJSON(*obj);

    EXPECT_EQ(parsed.getOperation(), Operation::APPEND);
    EXPECT_EQ(parsed.totals.records, original.totals.records);
    EXPECT_EQ(parsed.totals.data_files, original.totals.data_files);
    EXPECT_EQ(parsed.totals.files_size, original.totals.files_size);

    /// The parsed summary can drive the next snapshot's totals.
    SnapshotSummary next(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 1,
        .added_records = 2,
        .added_files_size = 823,
        .num_partitions = 1});
    next.applyTotals(parsed.totals);
    EXPECT_EQ(next.totals.records, 5);
    EXPECT_EQ(next.totals.data_files, 3);
}

TEST(IcebergSnapshotSummary, DeletePositionDeletesRoundTrip)
{
    /// Regression: `removed_position_deletes` was consumed by finalize but neither
    /// written nor read back, so it was silently lost on round-trip (read as 0).
    SnapshotSummary grandparent(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 3,
        .added_records = 50,
        .added_files_size = 1000,
        .num_partitions = 1});
    grandparent.applyTotals(std::nullopt);

    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateOverwrite{
        .added_delete_files = 2,
        .added_files_size = 100,
        .num_partitions = 1,
        .num_deleted_rows = 10});
    parent.applyTotals(grandparent.totals);
    EXPECT_EQ(parent.totals.position_deletes, 10);

    SnapshotSummary del(DB::Iceberg::SnapshotSummaryUpdateDelete{
        .removed_data_files = 0,
        .removed_records = 0,
        .removed_files_size = 50,
        .removed_position_delete_files = 1,
        .removed_position_deletes = 4,
        .num_partitions = 1});
    del.applyTotals(parent.totals);
    EXPECT_EQ(del.totals.position_deletes, 6);

    auto obj = del.toJSON();
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_position_delete_files), "1");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_position_deletes), "4");

    auto parsed = SnapshotSummary::fromJSON(*obj);
    const auto & parsed_delete = std::get<DB::Iceberg::SnapshotSummaryUpdateDelete>(parsed.update);
    EXPECT_EQ(parsed_delete.removed_position_delete_files, 1);
    EXPECT_EQ(parsed_delete.removed_position_deletes, 4);
}

TEST(IcebergSnapshotSummary, ReplaceAdjustsDataTotals)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 5, .added_records = 100, .added_files_size = 5000, .num_partitions = 3});
    parent.applyTotals(std::nullopt);

    /// Compaction: 5 small files (100 records, 5000 bytes) rewritten into
    /// 1 larger file with the same data but a different on-disk size.
    SnapshotSummary replace(DB::Iceberg::SnapshotSummaryUpdateReplace{
        .added_files = 1,
        .added_records = 100,
        .added_files_size = 4000,
        .removed_data_files = 5,
        .removed_records = 100,
        .removed_files_size = 5000,
        .num_partitions = 3});
    replace.applyTotals(parent.totals);

    EXPECT_EQ(replace.totals.records, 100);     // unchanged: same data
    EXPECT_EQ(replace.totals.data_files, 1);    // 5 - 5 + 1
    EXPECT_EQ(replace.totals.files_size, 4000); // 5000 - 5000 + 4000
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, ReplaceWithoutParentThrows)
{
    SnapshotSummary replace(DB::Iceberg::SnapshotSummaryUpdateReplace{
        .added_files = 1,
        .added_records = 100,
        .added_files_size = 4000,
        .removed_data_files = 5,
        .removed_records = 100,
        .removed_files_size = 5000,
        .num_partitions = 3});
    EXPECT_THROW(replace.applyTotals(std::nullopt), DB::Exception);
}
#endif

TEST(IcebergSnapshotSummary, ToJSONReplaceFields)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 5, .added_records = 100, .added_files_size = 5000, .num_partitions = 3});
    parent.applyTotals(std::nullopt);

    SnapshotSummary replace(DB::Iceberg::SnapshotSummaryUpdateReplace{
        .added_files = 1,
        .added_records = 100,
        .added_files_size = 4000,
        .removed_data_files = 5,
        .removed_records = 100,
        .removed_files_size = 5000,
        .num_partitions = 3});
    replace.applyTotals(parent.totals);

    auto obj = replace.toJSON();
    ASSERT_TRUE(obj);
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_operation), DB::Iceberg::f_replace);
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_data_files), "1");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_records), "100");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_files_size), "4000");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_data_files), "5");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_deleted_data_files), "5");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_deleted_records), "100");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_files_size), "5000");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_data_files), "1");
}

TEST(IcebergSnapshotSummary, ReplaceRoundTripThroughJSON)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 5, .added_records = 100, .added_files_size = 5000, .num_partitions = 3});
    parent.applyTotals(std::nullopt);

    SnapshotSummary replace(DB::Iceberg::SnapshotSummaryUpdateReplace{
        .added_files = 1,
        .added_records = 100,
        .added_files_size = 4000,
        .removed_data_files = 5,
        .removed_records = 100,
        .removed_files_size = 5000,
        .num_partitions = 3});
    replace.applyTotals(parent.totals);

    auto parsed = SnapshotSummary::fromJSON(*replace.toJSON());
    EXPECT_EQ(parsed.getOperation(), Operation::REPLACE);
    EXPECT_EQ(parsed.totals.records, replace.totals.records);
    EXPECT_EQ(parsed.totals.data_files, replace.totals.data_files);
    EXPECT_EQ(parsed.totals.files_size, replace.totals.files_size);
}

#endif
