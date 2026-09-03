#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>
#if !CLICKHOUSE_CLOUD
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#endif

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

#if !CLICKHOUSE_CLOUD
TEST(IcebergCompactionOverwriteClassification, PositionDeleteOnlyPredicate)
{
    /// Compaction may skip an `overwrite` snapshot only when it adds position delete files and nothing
    /// else, because it collects only data and position delete files: any other delta would be dropped from
    /// the rewritten table. Each conjunct has at least one row isolating it, satisfying every other
    /// conjunct, so dropping any single term admits a named row; some rows fail more than one term besides.
    /// The accounting proof is bounded above by the two rows declaring more position delete files than
    /// delete files and below by the three unaccounted ones. The refused shapes are constructed because
    /// ClickHouse's own writer cannot emit them (it never writes an equality delete or a removal counter),
    /// which is why they live here and not in a stateless test.
    struct Case
    {
        /// Constructor rather than in-class initializers: a default `skippable` would let a row omit
        /// its expected verdict and silently assert `false`.
        Case(const char * name_, DB::Iceberg::SnapshotSummaryUpdateOverwrite update_, bool skippable_)
            : name(name_), update(update_), skippable(skippable_)
        {
        }

        const char * name;
        DB::Iceberg::SnapshotSummaryUpdateOverwrite update;
        bool skippable;
    };

    const Case cases[] = {
        /// A foreign writer may omit the optional `added-position-delete-files` key. It reads as
        /// 0, so requiring equality with `added-delete-files` would refuse a history that the
        /// pre-fix guard accepted.
        {"foreign_position_delete_file_count_absent",
         {.added_delete_files = 1, .added_position_deletes = 1}, true},
        /// The reported defect: one delete file marking many rows.
        {"clickhouse_multi_row",
         {.added_delete_files = 1, .added_position_delete_files = 1, .added_position_deletes = 10}, true},
        /// A partitioned delete writes one position delete file per touched partition, so the accepted
        /// shape is not limited to a single delete file.
        {"clickhouse_multi_partition",
         {.added_delete_files = 2, .added_position_delete_files = 2, .added_position_deletes = 10}, true},
        {"clickhouse_single_row",
         {.added_delete_files = 1, .added_position_delete_files = 1, .added_position_deletes = 1}, true},
        /// A delete file marking no rows is still only a position delete file.
        {"position_delete_file_marking_zero_rows",
         {.added_delete_files = 1, .added_position_delete_files = 1}, true},

        /// No delete file declared at all: nothing identifies this as a position delete overwrite.
        {"no_delete_keys_at_all", {}, false},
        /// Equality deletes are not collected, so admitting these would resurrect deleted rows.
        {"equality_delete_files_and_rows",
         {.added_delete_files = 2,
          .added_position_delete_files = 1,
          .added_equality_delete_files = 1,
          .added_equality_deletes = 5}, false},
        {"equality_delete_row_count_only",
         {.added_delete_files = 1, .added_position_delete_files = 1, .added_equality_deletes = 5}, false},
        {"equality_delete_file_count_only",
         {.added_delete_files = 2, .added_position_delete_files = 1, .added_equality_delete_files = 1}, false},
        /// Accounted for via the absent-counter exception, so only the equality file-count term
        /// can refuse it.
        {"equality_delete_file_count_only_via_absent_counter",
         {.added_delete_files = 1, .added_position_deletes = 5, .added_equality_delete_files = 1}, false},
        /// Adding data files makes this a rewrite, not a row delete.
        {"data_files_added",
         {.added_files = 1, .added_delete_files = 1, .added_position_delete_files = 1}, false},
        /// `added-records` is a separate optional key from `added-data-files`, so a writer may
        /// declare added rows while omitting the file count. Records were added either way.
        {"data_records_added_without_file_count",
         {.added_records = 10, .added_delete_files = 1, .added_position_delete_files = 1, .added_position_deletes = 10}, false},
        /// More position delete files than delete files is incoherent metadata.
        {"position_delete_file_count_exceeds_delete_file_count",
         {.added_delete_files = 1, .added_position_delete_files = 2}, false},
        /// Same shape with rows declared, so only the exception's own file-count term refuses it:
        /// the exception requires the count to be absent, not merely unequal.
        {"position_delete_file_count_exceeds_with_rows_declared",
         {.added_delete_files = 1, .added_position_delete_files = 2, .added_position_deletes = 5}, false},
        /// A summary that declares delete files but accounts for none of them proves nothing:
        /// the equality counters are optional too, so their absence is not evidence.
        {"aggregate_delete_files_only_unaccounted",
         {.added_delete_files = 1}, false},
        /// Two delete files, one accounted for as a position delete: the second may be an
        /// equality delete whose optional breakdown keys the writer omitted.
        {"delete_files_partially_accounted",
         {.added_delete_files = 2, .added_position_delete_files = 1, .added_position_deletes = 5}, false},
        {"delete_files_partially_accounted_file_count_absent",
         {.added_delete_files = 2, .added_position_deletes = 5}, false},
        /// Removals are refused: compaction never drops a data file collected from an earlier
        /// snapshot, so a removed file would be resurrected.
        {"data_file_removal_declared",
         {.added_delete_files = 1, .added_position_delete_files = 1, .deleted_data_files = 1}, false},
        {"removed_record_count_declared",
         {.added_delete_files = 1, .added_position_delete_files = 1, .removed_records = 5}, false},
        {"removed_file_size_declared",
         {.added_delete_files = 1, .added_position_delete_files = 1, .removed_files_size = 100}, false},
    };

    for (const auto & c : cases)
        EXPECT_EQ(DB::Iceberg::overwriteIsPositionDeleteOnly(c.update), c.skippable) << "case: " << c.name;
}

TEST(IcebergCompactionOverwriteClassification, HistoryGuardConsultsThePredicate)
{
    /// The predicate above is only useful if the history guard actually calls it, so drive the
    /// guard itself: a skippable overwrite must yield no append delta, and a refused one must
    /// throw. Without this, reverting the call site would leave every row above green.
    const auto record_for = [](DB::Iceberg::SnapshotSummaryUpdateOverwrite update)
    {
        DB::Iceberg::IcebergHistoryRecord record;
        record.snapshot_id = 1;
        record.snapshot_summary = SnapshotSummary(
            std::move(update),
            SnapshotSummary(DB::Iceberg::SnapshotSummaryUpdateAppend{
                .added_files = 3, .added_records = 30, .added_files_size = 1000, .num_partitions = 1}).getTotals());
        return record;
    };

    const auto skippable = record_for({.added_delete_files = 1, .added_position_delete_files = 1, .added_position_deletes = 10});
    EXPECT_FALSE(DB::Iceberg::tryGetAppendUpdate(skippable).has_value());

    const auto refused = record_for({.added_delete_files = 1, .added_position_delete_files = 1, .added_equality_deletes = 5});
    EXPECT_THROW(static_cast<void>(DB::Iceberg::tryGetAppendUpdate(refused)), DB::Exception);

    /// An append is replayed rather than skipped, so a guard that answered `nullopt` for
    /// everything would fail here too.
    DB::Iceberg::IcebergHistoryRecord append_record;
    append_record.snapshot_id = 2;
    append_record.snapshot_summary = SnapshotSummary(DB::Iceberg::SnapshotSummaryUpdateAppend{
        .added_files = 2, .added_records = 20, .added_files_size = 500, .num_partitions = 1});
    const auto append = DB::Iceberg::tryGetAppendUpdate(append_record);
    ASSERT_TRUE(append.has_value());
    EXPECT_EQ(append->added_files, 2);
}
#endif

#endif
