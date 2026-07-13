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

    EXPECT_EQ(summary.getTotals().records, 3);
    EXPECT_EQ(summary.getTotals().data_files, 2);
    EXPECT_EQ(summary.getTotals().files_size, 1638);
    EXPECT_EQ(summary.getTotals().delete_files, 0);
    EXPECT_EQ(summary.getTotals().position_deletes, 0);
    EXPECT_EQ(summary.getTotals().equality_deletes, 0);
}

TEST(IcebergSnapshotSummary, AppendAccumulatesOnParent)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 2, .added_records = 3, .added_files_size = 1638, .num_partitions = 2});

    SnapshotSummary next(
        DB::Iceberg::SnapshotSummaryUpdateAppend{
            .added_files = 1,
            .added_records = 2,
            .added_files_size = 823,
            .num_partitions = 1},
        parent.getTotals());

    EXPECT_EQ(next.getTotals().records, 5);
    EXPECT_EQ(next.getTotals().data_files, 3);
    EXPECT_EQ(next.getTotals().files_size, 2461);
}

TEST(IcebergSnapshotSummary, DeleteSubtractsFromParent)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 3, .added_records = 5, .added_files_size = 2461, .num_partitions = 3});

    SnapshotSummary del(
        DB::Iceberg::SnapshotSummaryUpdateDelete{
            .deleted_data_files = 1,
            .removed_records = 2,
            .removed_files_size = 823,
            .removed_position_delete_files = 0,
            .removed_position_deletes = 0,
            .num_partitions = 1},
        parent.getTotals());

    EXPECT_EQ(del.getTotals().records, 3);
    EXPECT_EQ(del.getTotals().data_files, 2);
    EXPECT_EQ(del.getTotals().files_size, 1638);
}

TEST(IcebergSnapshotSummary, OverwriteAddsDeleteCounts)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 3, .added_records = 5, .added_files_size = 2461, .num_partitions = 3});

    SnapshotSummary ow(
        DB::Iceberg::SnapshotSummaryUpdateOverwrite{
            .added_files_size = 100,
            .added_delete_files = 1,
            .added_position_deletes = 4,
            .num_partitions = 1},
        parent.getTotals());

    EXPECT_EQ(ow.getTotals().delete_files, 1);
    EXPECT_EQ(ow.getTotals().position_deletes, 4);
    EXPECT_EQ(ow.getTotals().data_files, 3);  // unchanged: overwrite doesn't touch data file count
    EXPECT_EQ(ow.getTotals().records, 5);     // unchanged: overwrite doesn't add data records
    EXPECT_EQ(ow.getTotals().files_size, 2561);
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, DeleteWithoutParentThrows)
{
    EXPECT_THROW(
        SnapshotSummary(DB::Iceberg::SnapshotSummaryUpdateDelete{
            .deleted_data_files = 1,
            .removed_records = 1,
            .removed_files_size = 100,
            .removed_position_delete_files = 0,
            .removed_position_deletes = 0,
            .num_partitions = 1}),
        DB::Exception);
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, OverwriteWithoutParentThrows)
{
    EXPECT_THROW(
        SnapshotSummary(DB::Iceberg::SnapshotSummaryUpdateOverwrite{
            .added_files_size = 100,
            .added_delete_files = 1,
            .added_position_deletes = 1,
            .num_partitions = 1}),
        DB::Exception);
}
#endif

TEST(IcebergSnapshotSummary, ToJSONAppendFields)
{
    SnapshotSummary summary(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 2, .added_records = 3, .added_files_size = 1638, .num_partitions = 2});

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

    SnapshotSummary del(
        DB::Iceberg::SnapshotSummaryUpdateDelete{
            .deleted_data_files = 1,
            .removed_records = 2,
            .removed_files_size = 823,
            .removed_position_delete_files = 0,
            .removed_position_deletes = 0,
            .num_partitions = 1},
        parent.getTotals());

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

    auto obj = original.toJSON();
    auto parsed = SnapshotSummary::fromJSON(*obj);
    ASSERT_TRUE(parsed.has_value());

    EXPECT_EQ(parsed->getOperation(), Operation::APPEND);
    EXPECT_EQ(parsed->getTotals().records, original.getTotals().records);
    EXPECT_EQ(parsed->getTotals().data_files, original.getTotals().data_files);
    EXPECT_EQ(parsed->getTotals().files_size, original.getTotals().files_size);

    /// The parsed summary can drive the next snapshot's totals.
    SnapshotSummary next(
        DB::Iceberg::SnapshotSummaryUpdateAppend{
            .added_files = 1,
            .added_records = 2,
            .added_files_size = 823,
            .num_partitions = 1},
        parsed->getTotals());
    EXPECT_EQ(next.getTotals().records, 5);
    EXPECT_EQ(next.getTotals().data_files, 3);
}

TEST(IcebergSnapshotSummary, DeletePositionDeletesRoundTrip)
{
    /// Regression: `removed_position_deletes` was consumed when computing totals but neither
    /// written nor read back, so it was silently lost on round-trip (read as 0).
    SnapshotSummary grandparent(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 3,
        .added_records = 50,
        .added_files_size = 1000,
        .num_partitions = 1});

    SnapshotSummary parent(
        DB::Iceberg::SnapshotSummaryUpdateOverwrite{
            .added_files_size = 100,
            .added_delete_files = 2,
            .added_position_deletes = 10,
            .num_partitions = 1},
        grandparent.getTotals());
    EXPECT_EQ(parent.getTotals().position_deletes, 10);

    SnapshotSummary del(
        DB::Iceberg::SnapshotSummaryUpdateDelete{
            .deleted_data_files = 0,
            .removed_records = 0,
            .removed_files_size = 50,
            .removed_position_delete_files = 1,
            .removed_position_deletes = 4,
            .num_partitions = 1},
        parent.getTotals());
    EXPECT_EQ(del.getTotals().position_deletes, 6);

    auto obj = del.toJSON();
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_position_delete_files), "1");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_position_deletes), "4");

    auto parsed = SnapshotSummary::fromJSON(*obj);
    ASSERT_TRUE(parsed.has_value());
    const auto & parsed_delete = parsed->getUpdate<DB::Iceberg::SnapshotSummaryUpdateDelete>();
    EXPECT_EQ(parsed_delete.removed_position_delete_files, 1);
    EXPECT_EQ(parsed_delete.removed_position_deletes, 4);
}

TEST(IcebergSnapshotSummary, ReplaceAdjustsDataTotals)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 5, .added_records = 100, .added_files_size = 5000, .num_partitions = 3});

    /// Compaction: 5 small files (100 records, 5000 bytes) rewritten into
    /// 1 larger file with the same data but a different on-disk size.
    SnapshotSummary replace(
        DB::Iceberg::SnapshotSummaryUpdateReplace{
            .added_files = 1,
            .added_records = 100,
            .added_files_size = 4000,
            .deleted_data_files = 5,
            .removed_records = 100,
            .removed_files_size = 5000,
            .num_partitions = 3},
        parent.getTotals());

    EXPECT_EQ(replace.getTotals().records, 100);     // unchanged: same data
    EXPECT_EQ(replace.getTotals().data_files, 1);    // 5 - 5 + 1
    EXPECT_EQ(replace.getTotals().files_size, 4000); // 5000 - 5000 + 4000
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(IcebergSnapshotSummary, ReplaceWithoutParentThrows)
{
    EXPECT_THROW(
        SnapshotSummary(DB::Iceberg::SnapshotSummaryUpdateReplace{
            .added_files = 1,
            .added_records = 100,
            .added_files_size = 4000,
            .deleted_data_files = 5,
            .removed_records = 100,
            .removed_files_size = 5000,
            .num_partitions = 3}),
        DB::Exception);
}
#endif

TEST(IcebergSnapshotSummary, ToJSONReplaceFields)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 5, .added_records = 100, .added_files_size = 5000, .num_partitions = 3});

    SnapshotSummary replace(
        DB::Iceberg::SnapshotSummaryUpdateReplace{
            .added_files = 1,
            .added_records = 100,
            .added_files_size = 4000,
            .deleted_data_files = 5,
            .removed_records = 100,
            .removed_files_size = 5000,
            .num_partitions = 3},
        parent.getTotals());

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
    SnapshotSummary parent(
        DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 5, .added_records = 100, .added_files_size = 5000, .num_partitions = 3});

    SnapshotSummary replace(
        DB::Iceberg::SnapshotSummaryUpdateReplace{
            .added_files = 1,
            .added_records = 100,
            .added_files_size = 4000,
            .deleted_data_files = 5,
            .removed_records = 100,
            .removed_files_size = 5000,
            .num_partitions = 3},
        parent.getTotals());

    auto parsed = SnapshotSummary::fromJSON(*replace.toJSON());
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->getOperation(), Operation::REPLACE);
    EXPECT_EQ(parsed->getTotals().records, replace.getTotals().records);
    EXPECT_EQ(parsed->getTotals().data_files, replace.getTotals().data_files);
    EXPECT_EQ(parsed->getTotals().files_size, replace.getTotals().files_size);
}

TEST(IcebergSnapshotSummary, OverwriteAddsEqualityDeleteCounts)
{
    SnapshotSummary parent(DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 3, .added_records = 5, .added_files_size = 2461, .num_partitions = 3});

    /// Upsert-style overwrite that adds both position and equality delete files.
    SnapshotSummary ow(
        DB::Iceberg::SnapshotSummaryUpdateOverwrite{
            .added_files_size = 100,
            .added_delete_files = 3, // 1 position + 2 equality
            .added_position_delete_files = 1,
            .added_position_deletes = 4,
            .added_equality_delete_files = 2,
            .added_equality_deletes = 7,
            .num_partitions = 1},
        parent.getTotals());

    EXPECT_EQ(ow.getTotals().delete_files, 3);
    EXPECT_EQ(ow.getTotals().position_deletes, 4);
    EXPECT_EQ(ow.getTotals().equality_deletes, 7);
    EXPECT_EQ(ow.getTotals().data_files, 3); // unchanged: overwrite doesn't touch data file count

    auto obj = ow.toJSON();
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_equality_delete_files), "2");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_added_equality_deletes), "7");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_equality_deletes), "7");
}

TEST(IcebergSnapshotSummary, EqualityDeletesRoundTrip)
{
    /// Regression: the constructor hardcoded `totals.equality_deletes = 0`, which silently wiped
    /// any equality-delete total inherited from the parent snapshot.
    SnapshotSummary grandparent(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 3,
        .added_records = 50,
        .added_files_size = 1000,
        .num_partitions = 1});

    SnapshotSummary parent(
        DB::Iceberg::SnapshotSummaryUpdateOverwrite{
            .added_files_size = 100,
            .added_delete_files = 2,
            .added_equality_delete_files = 2,
            .added_equality_deletes = 10,
            .num_partitions = 1},
        grandparent.getTotals());
    EXPECT_EQ(parent.getTotals().equality_deletes, 10);
    EXPECT_EQ(parent.getTotals().delete_files, 2);

    SnapshotSummary del(
        DB::Iceberg::SnapshotSummaryUpdateDelete{
            .removed_files_size = 50,
            .removed_equality_delete_files = 1,
            .removed_equality_deletes = 4,
            .num_partitions = 1},
        parent.getTotals());
    EXPECT_EQ(del.getTotals().equality_deletes, 6);
    EXPECT_EQ(del.getTotals().delete_files, 1);

    auto obj = del.toJSON();
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_equality_delete_files), "1");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_removed_equality_deletes), "4");
    EXPECT_EQ(obj->getValue<std::string>(DB::Iceberg::f_total_equality_deletes), "6");

    auto parsed = SnapshotSummary::fromJSON(*obj);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->getTotals().equality_deletes, 6);
    const auto & parsed_delete = parsed->getUpdate<DB::Iceberg::SnapshotSummaryUpdateDelete>();
    EXPECT_EQ(parsed_delete.removed_equality_delete_files, 1);
    EXPECT_EQ(parsed_delete.removed_equality_deletes, 4);
}

TEST(IcebergSnapshotSummary, ReplaceRewritesDeleteFiles)
{
    /// A replace that compacts delete files: it removes old position + equality delete files and
    /// writes fewer new ones, while leaving the logical table data unchanged.
    SnapshotSummary grandparent(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 5, .added_records = 100, .added_files_size = 5000, .num_partitions = 3});

    SnapshotSummary parent(
        DB::Iceberg::SnapshotSummaryUpdateOverwrite{
            .added_files_size = 200,
            .added_delete_files = 5, // 3 position + 2 equality
            .added_position_delete_files = 3,
            .added_position_deletes = 30,
            .added_equality_delete_files = 2,
            .added_equality_deletes = 20,
            .num_partitions = 3},
        grandparent.getTotals());
    EXPECT_EQ(parent.getTotals().delete_files, 5);
    EXPECT_EQ(parent.getTotals().position_deletes, 30);
    EXPECT_EQ(parent.getTotals().equality_deletes, 20);

    SnapshotSummary replace(
        DB::Iceberg::SnapshotSummaryUpdateReplace{
            .added_files_size = 80,
            .added_delete_files = 2, // 1 position + 1 equality
            .added_position_delete_files = 1,
            .added_position_deletes = 30,
            .added_equality_delete_files = 1,
            .added_equality_deletes = 20,
            .removed_files_size = 200,
            .removed_delete_files = 5,
            .removed_position_delete_files = 3,
            .removed_position_deletes = 30,
            .removed_equality_delete_files = 2,
            .removed_equality_deletes = 20,
            .num_partitions = 3},
        parent.getTotals());

    EXPECT_EQ(replace.getTotals().delete_files, 2);     // 5 - 5 + 2
    EXPECT_EQ(replace.getTotals().position_deletes, 30); // unchanged: same deletes, fewer files
    EXPECT_EQ(replace.getTotals().equality_deletes, 20); // unchanged: same deletes, fewer files

    auto parsed = SnapshotSummary::fromJSON(*replace.toJSON());
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->getOperation(), Operation::REPLACE);
    EXPECT_EQ(parsed->getTotals().delete_files, 2);
    EXPECT_EQ(parsed->getTotals().position_deletes, 30);
    EXPECT_EQ(parsed->getTotals().equality_deletes, 20);
    const auto & parsed_replace = parsed->getUpdate<DB::Iceberg::SnapshotSummaryUpdateReplace>();
    EXPECT_EQ(parsed_replace.added_equality_delete_files, 1);
    EXPECT_EQ(parsed_replace.added_equality_deletes, 20);
    EXPECT_EQ(parsed_replace.removed_equality_delete_files, 2);
    EXPECT_EQ(parsed_replace.removed_equality_deletes, 20);
}

TEST(IcebergSnapshotSummary, AppendPreservesInheritedEqualityDeletes)
{
    /// An append touches no delete files, so it must carry the parent's equality-delete total
    /// forward instead of resetting it to zero.
    SnapshotSummary grandparent(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 3, .added_records = 50, .added_files_size = 1000, .num_partitions = 1});

    SnapshotSummary parent(
        DB::Iceberg::SnapshotSummaryUpdateOverwrite{
            .added_files_size = 100,
            .added_delete_files = 2,
            .added_equality_delete_files = 2,
            .added_equality_deletes = 10,
            .num_partitions = 1},
        grandparent.getTotals());
    EXPECT_EQ(parent.getTotals().equality_deletes, 10);

    SnapshotSummary append(
        DB::Iceberg::SnapshotSummaryUpdateAppend{.added_files = 1, .added_records = 5, .added_files_size = 200, .num_partitions = 1},
        parent.getTotals());

    EXPECT_EQ(append.getTotals().equality_deletes, 10); // preserved, not reset to 0
    EXPECT_EQ(append.getTotals().delete_files, 2);       // preserved
    EXPECT_EQ(append.getTotals().records, 55);
}

#endif
