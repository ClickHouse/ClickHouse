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
using Operation = SnapshotSummary::Operation;
}

TEST(IcebergSnapshotSummary, AppendFirstSnapshotTotalsEqualDeltas)
{
    /// Regression: when a fresh table has no parent snapshot, the totals must
    /// equal the deltas, not zero and not 2 x deltas.
    auto summary = SnapshotSummary::createAppend(
        /*added_files=*/ 2,
        /*added_records=*/ 3,
        /*added_files_size=*/ 1638,
        /*num_partitions=*/ 2);

    summary.finalize(/*parent=*/ std::nullopt);

    EXPECT_EQ(summary.total_records, 3);
    EXPECT_EQ(summary.total_data_files, 2);
    EXPECT_EQ(summary.total_files_size, 1638);
    EXPECT_EQ(summary.total_delete_files, 0);
    EXPECT_EQ(summary.total_position_deletes, 0);
    EXPECT_EQ(summary.total_equality_deletes, 0);
}

TEST(IcebergSnapshotSummary, AppendAccumulatesOnParent)
{
    auto parent = SnapshotSummary::createAppend(2, 3, 1638, 2);
    parent.finalize(std::nullopt);

    auto next = SnapshotSummary::createAppend(
        /*added_files=*/ 1,
        /*added_records=*/ 2,
        /*added_files_size=*/ 823,
        /*num_partitions=*/ 1);
    next.finalize(parent);

    EXPECT_EQ(next.total_records, 5);
    EXPECT_EQ(next.total_data_files, 3);
    EXPECT_EQ(next.total_files_size, 2461);
}

TEST(IcebergSnapshotSummary, DeleteSubtractsFromParent)
{
    auto parent = SnapshotSummary::createAppend(3, 5, 2461, 3);
    parent.finalize(std::nullopt);

    auto del = SnapshotSummary::createDelete(
        /*removed_data_files=*/ 1,
        /*removed_records=*/ 2,
        /*removed_files_size=*/ 823,
        /*removed_position_delete_files=*/ 0,
        /*removed_position_deletes=*/ 0,
        /*num_partitions=*/ 1);
    del.finalize(parent);

    EXPECT_EQ(del.total_records, 3);
    EXPECT_EQ(del.total_data_files, 2);
    EXPECT_EQ(del.total_files_size, 1638);
}

TEST(IcebergSnapshotSummary, OverwriteAddsDeleteCounts)
{
    auto parent = SnapshotSummary::createAppend(3, 5, 2461, 3);
    parent.finalize(std::nullopt);

    auto ow = SnapshotSummary::createOverwrite(
        /*added_delete_files=*/ 1,
        /*added_files_size=*/ 100,
        /*num_partitions=*/ 1,
        /*num_deleted_rows=*/ 4);
    ow.finalize(parent);

    EXPECT_EQ(ow.total_delete_files, 1);
    EXPECT_EQ(ow.total_position_deletes, 4);
    EXPECT_EQ(ow.total_data_files, 3);  // unchanged: overwrite doesn't touch data file count
    EXPECT_EQ(ow.total_records, 5);     // unchanged: overwrite doesn't add data records
    EXPECT_EQ(ow.total_files_size, 2561);
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, DeleteWithoutParentThrows)
{
    auto del = SnapshotSummary::createDelete(1, 1, 100, 0, 0, 1);
    EXPECT_THROW(del.finalize(std::nullopt), DB::Exception);
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, OverwriteWithoutParentThrows)
{
    auto ow = SnapshotSummary::createOverwrite(1, 100, 1, 1);
    EXPECT_THROW(ow.finalize(std::nullopt), DB::Exception);
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, DoubleFinalizeThrows)
{
    auto summary = SnapshotSummary::createAppend(1, 1, 100, 1);
    summary.finalize(std::nullopt);
    EXPECT_THROW(summary.finalize(std::nullopt), DB::Exception);
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, ToJSONBeforeFinalizeThrows)
{
    auto summary = SnapshotSummary::createAppend(1, 1, 100, 1);
    EXPECT_THROW((void)summary.toJSON(), DB::Exception);
}
#endif

TEST(IcebergSnapshotSummary, ToJSONAppendFields)
{
    auto summary = SnapshotSummary::createAppend(2, 3, 1638, 2);
    summary.finalize(std::nullopt);

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
    auto parent = SnapshotSummary::createAppend(3, 5, 2461, 3);
    parent.finalize(std::nullopt);

    auto del = SnapshotSummary::createDelete(1, 2, 823, 0, 0, 1);
    del.finalize(parent);

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
    auto original = SnapshotSummary::createAppend(2, 3, 1638, 2);
    original.finalize(std::nullopt);

    auto obj = original.toJSON();
    auto parsed = SnapshotSummary::fromJSON(*obj);

    EXPECT_EQ(parsed.operation, Operation::APPEND);
    EXPECT_EQ(parsed.total_records, original.total_records);
    EXPECT_EQ(parsed.total_data_files, original.total_data_files);
    EXPECT_EQ(parsed.total_files_size, original.total_files_size);

    /// The parsed summary can drive the next snapshot's totals.
    auto next = SnapshotSummary::createAppend(1, 2, 823, 1);
    next.finalize(parsed);
    EXPECT_EQ(next.total_records, 5);
    EXPECT_EQ(next.total_data_files, 3);
}

#endif
