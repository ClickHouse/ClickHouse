/// Read-side snapshot-consistency: a part NEWER than the pinned snapshot csn
/// must be excluded from the read.
///
/// README invariant: a reader pinned at csn C sees only parts with
/// `creation_csn ≤ C` (and, per kept part, the bitmap with max csn ≤ C). Both
/// paths now take the per-partition snapshot map pinned at part-selection time
/// (`MergeTreeData::captureUniqueKeyPartitionSnapshots()`); the count path
/// (`MergeTreeData::getDeadRowsForUniqueKey`) and the SELECT path
/// (`UniqueKeyReadFilter::applyUniqueKeyDeleteBitmaps`) share the per-part
/// predicate `isPartVisibleAtSnapshotCsn`. These tests drive BOTH REAL paths
/// over a real `StorageMergeTree`:
///   - count path: a part whose `creation_csn` exceeds the snapshot's pinned
///     csn contributes ALL its rows as dead (invisible at C), so a count over
///     it returns 0 live rows;
///   - SELECT path: `applyUniqueKeyDeleteBitmaps` erases a too-new part from
///     `RangesInDataParts` and keeps a boundary (`== C`) part.
/// Pre-fix the too-new part is wrongly retained / treated as fully live on the
/// respective path. A partition ABSENT from the snapshot map (no controller at
/// pin time) is fully live on both paths — covered explicitly below.
#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyMarkerPart.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyReadFilter.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>
#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_uk_storage_harness.h>
#include <Storages/StorageMergeTree.h>

#include <Common/Logger.h>

#include <unordered_map>

using namespace DB;
using namespace DB::UniqueKeyTxn;
using namespace DB::UniqueKeyTxn::tests;

namespace
{

/// Build a real (in-memory) data part in partition "all" with a known
/// `rows_count` and `creation_csn`. Reuses `createMarkerPart` for the part
/// skeleton (it requires `is_marker`, so the manifest is a marker), then
/// overrides the scalars the visibility predicate reads: `rows_count` and the
/// in-memory `getUniqueKeyMeta()->creation_csn` (the predicate ignores
/// `is_marker`).
MergeTreeData::DataPartPtr makePartWithCsn(
    UKStorageHarness & h, Int64 block_number, size_t rows, CSN creation_csn)
{
    UniqueKeyManifest meta;
    meta.creation_csn = creation_csn;
    meta.is_marker = true;  /// `createMarkerPart` asserts this; irrelevant to the csn predicate
    auto handle = createMarkerPart(*h.storage, /*partition_id=*/"all", block_number, MergeTreePartition{}, meta);
    handle.data_part->rows_count = rows;
    handle.data_part->setUniqueKeyMeta(UniqueKeyPartMeta{creation_csn, /*is_marker=*/false});
    /// One data mark of `rows` rows, no final mark — gives the part a valid
    /// granularity so the SELECT path's `getRowsCount()` over `MarkRange{0,1}`
    /// is well-defined (a raw marker part has 0 marks).
    handle.data_part->index_granularity = std::make_shared<MergeTreeIndexGranularityConstant>(
        /*constant_granularity=*/rows, /*last_mark_granularity=*/rows,
        /*num_marks_without_final=*/1, /*has_final_mark=*/false);
    return handle.data_part;
}

/// Build a snapshot map containing partition "all" pinned at the freshly-seeded
/// csn=0, for the consumer-side tests (`getDeadRowsForUniqueKey` /
/// `applyUniqueKeyDeleteBitmaps`) that exercise filtering GIVEN a populated map.
/// We pin the controller directly rather than via
/// `captureUniqueKeyPartitionSnapshots()`, because that enumerates ACTIVE
/// partitions and this harness's detached marker parts are not in the active
/// set — the enumeration path is exercised separately by
/// `CaptureSnapshotsPinsActivePartitionWithoutExistingController`.
std::unordered_map<String, QuerySnapshot> pinAllPartition(UKStorageHarness & h)
{
    std::unordered_map<String, QuerySnapshot> snapshots;
    snapshots.emplace("all", h.storage->getOrCreateTxnController("all").takeQuerySnapshot());
    return snapshots;
}

/// Commit a freshly-built empty part into the table's ACTIVE set so
/// `getAllPartitionIds()` reports its partition. Mirrors the writer path
/// (`renameTempPartAndReplace` + `Transaction::commit`). Used to prove the
/// capture enumerates active partitions, not just instantiated controllers.
MergeTreeData::DataPartPtr makeActivePart(UKStorageHarness & h, Int64 block_number)
{
    UniqueKeyManifest meta;
    meta.creation_csn = 0;
    meta.is_marker = true;  /// `createMarkerPart` asserts this; the active-set membership is what matters here
    auto handle = createMarkerPart(*h.storage, /*partition_id=*/"all", block_number, MergeTreePartition{}, meta);

    MergeTreeData::MutableDataPartPtr part = handle.data_part;
    MergeTreeData::Transaction txn(*h.storage, /*txn=*/nullptr);
    h.storage->renameTempPartAndReplace(part, txn, /*rename_in_transaction=*/true);
    /// `rename_in_transaction=true` defers the on-disk rename to `renameParts()`;
    /// `commit()` LOGICAL_ERRORs ("Parts had not been renamed") without it.
    txn.renameParts();
    txn.commit();
    return part;
}

}

/// A part whose `creation_csn` exceeds the snapshot's pinned csn is invisible:
/// `getDeadRowsForUniqueKey` counts ALL its rows dead (so a count nets to 0
/// live). The fresh controller (no active parts) seeds csn=0, so a part at
/// `creation_csn=5` is newer than the snapshot pinned at 0 → excluded.
TEST(ReadSnapshotCsnFilter, PartNewerThanSnapshotIsExcludedFromDeadCount)
{
    UKStorageHarness h({.with_unique_key = true, .table_name = "test_read_csn_filter", .relative_path = "store/test_read_csn_filter/"});

    /// rows=10, creation_csn=5; the partition has no active parts, so the txn
    /// controller seeds csn=0 and the pinned snapshot is at 0 < 5.
    auto newer_part = makePartWithCsn(h, /*block_number=*/5, /*rows=*/10, /*creation_csn=*/5);
    ASSERT_TRUE(newer_part->getUniqueKeyMeta().has_value());
    ASSERT_EQ(newer_part->getUniqueKeyMeta()->creation_csn, 5u);

    /// All 10 rows are dead at the pinned csn (part is invisible). Pre-fix this
    /// returns 0 (no creation_csn filter; the empty bitmap kills nothing).
    const auto snapshots = pinAllPartition(h);
    const size_t dead = h.storage->getDeadRowsForUniqueKey({newer_part}, snapshots);
    EXPECT_EQ(dead, 10u);
}

/// A part at or below the snapshot csn stays visible: with no delete bitmap its
/// dead count is 0 (all rows live). Guards against the filter over-excluding.
TEST(ReadSnapshotCsnFilter, PartAtOrBelowSnapshotStaysVisible)
{
    UKStorageHarness h({.with_unique_key = true, .table_name = "test_read_csn_filter", .relative_path = "store/test_read_csn_filter/"});

    /// creation_csn=0 (≤ the pinned csn=0) → visible; no bitmap → 0 dead.
    auto visible_part = makePartWithCsn(h, /*block_number=*/1, /*rows=*/10, /*creation_csn=*/0);
    const auto snapshots = pinAllPartition(h);
    const size_t dead = h.storage->getDeadRowsForUniqueKey({visible_part}, snapshots);
    EXPECT_EQ(dead, 0u);
}

/// Partition ABSENT from the snapshot map (did not exist at pin time): the part
/// is fully live on the count path even though its `creation_csn` exceeds any
/// csn we could have pinned. The boundary-pin fix must NOT lazily pin here — a
/// gap-DELETE is post-snapshot and ignored.
TEST(ReadSnapshotCsnFilter, PartitionNotInSnapshotMapIsFullyLiveForCount)
{
    UKStorageHarness h({.with_unique_key = true, .table_name = "test_read_csn_filter", .relative_path = "store/test_read_csn_filter/"});

    auto newer_part = makePartWithCsn(h, /*block_number=*/5, /*rows=*/10, /*creation_csn=*/5);
    /// Empty map: "all" is not present ⇒ 0 dead regardless of creation_csn.
    const std::unordered_map<String, QuerySnapshot> no_snapshots;
    const size_t dead = h.storage->getDeadRowsForUniqueKey({newer_part}, no_snapshots);
    EXPECT_EQ(dead, 0u);
}

/// DISCRIMINATING (B1 BLOCKER): `captureUniqueKeyPartitionSnapshots()` must pin
/// every ACTIVE partition, not just partitions whose controller was already
/// instantiated. We commit an active part in "all" WITHOUT ever touching its
/// txn controller (the post-restart shape: an active partition whose committed
/// DELETE state would live on disk, with no in-memory controller yet). The
/// capture must instantiate + pin it, so "all" is present in the map. Under the
/// pre-fix helper (iterate `unique_key_txn_controllers`), the map is empty here
/// — a partition with on-disk delete bitmaps would skip its bitmap filter and
/// resurface deleted rows.
TEST(ReadSnapshotCsnFilter, CaptureSnapshotsPinsActivePartitionWithoutExistingController)
{
    UKStorageHarness h({.with_unique_key = true, .table_name = "test_read_csn_filter", .relative_path = "store/test_read_csn_filter/"});

    /// Commit an active part; do NOT call getOrCreateTxnController beforehand.
    auto active_part = makeActivePart(h, /*block_number=*/1);
    ASSERT_FALSE(h.storage->getAllPartitionIds().empty());
    ASSERT_TRUE(h.storage->getAllPartitionIds().contains("all"));

    const auto snapshots = h.storage->captureUniqueKeyPartitionSnapshots();

    /// The active partition was instantiated + pinned by the capture itself.
    EXPECT_TRUE(snapshots.contains("all"));
    /// And the pinned snapshot is usable: a part newer than its csn is dead.
    auto newer_part = makePartWithCsn(h, /*block_number=*/9, /*rows=*/10, /*creation_csn=*/5);
    EXPECT_EQ(h.storage->getDeadRowsForUniqueKey({newer_part}, snapshots), 10u);
}

/// SELECT path: `applyUniqueKeyDeleteBitmaps` must DROP a part newer than the
/// pinned snapshot csn and KEEP a boundary part (`creation_csn == C`). The
/// controller seeds csn=0 (no active parts), so the snapshot pins at C=0: the
/// `creation_csn=1` part is too-new (dropped), the `creation_csn=0` part is at
/// the boundary (kept). Pre-fix BOTH parts survive (no SELECT-side filter).
TEST(ReadSnapshotCsnFilter, ApplyDeleteBitmapsDropsPartNewerThanSnapshot)
{
    UKStorageHarness h({.with_unique_key = true, .table_name = "test_read_csn_filter", .relative_path = "store/test_read_csn_filter/"});

    /// Boundary part (kept) and too-new part (dropped), one mark range each.
    auto boundary_part = makePartWithCsn(h, /*block_number=*/1, /*rows=*/10, /*creation_csn=*/0);
    auto newer_part = makePartWithCsn(h, /*block_number=*/2, /*rows=*/10, /*creation_csn=*/1);

    RangesInDataParts parts_with_ranges;
    {
        RangesInDataPart r(boundary_part);
        r.ranges = MarkRanges{MarkRange{0, 1}};
        parts_with_ranges.push_back(std::move(r));
    }
    {
        RangesInDataPart r(newer_part);
        r.ranges = MarkRanges{MarkRange{0, 1}};
        parts_with_ranges.push_back(std::move(r));
    }

    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    size_t sum_rows = 0;
    auto log = getLogger("ReadSnapshotCsnFilterTest");

    const auto snapshots = pinAllPartition(h);
    auto pins = applyUniqueKeyDeleteBitmaps(snapshots, parts_with_ranges, log, sum_marks, sum_ranges, sum_rows);

    /// Only the boundary part survives; the too-new part was erased.
    ASSERT_EQ(parts_with_ranges.size(), 1u);
    EXPECT_EQ(parts_with_ranges[0].data_part->name, boundary_part->name);
    EXPECT_FALSE(parts_with_ranges[0].ranges.empty());
    /// One shared pin per touched partition (single partition "all" here).
    ASSERT_NE(pins, nullptr);
    EXPECT_EQ(pins->size(), 1u);
}

/// SELECT path, partition ABSENT from the snapshot map: both parts are fully
/// live (no csn filter, no bitmap) — the empty map means no controller existed
/// at pin time, so no pre-snapshot DELETE can apply. No pins are returned.
TEST(ReadSnapshotCsnFilter, ApplyDeleteBitmapsKeepsAllPartsWhenPartitionNotInMap)
{
    UKStorageHarness h({.with_unique_key = true, .table_name = "test_read_csn_filter", .relative_path = "store/test_read_csn_filter/"});

    auto boundary_part = makePartWithCsn(h, /*block_number=*/1, /*rows=*/10, /*creation_csn=*/0);
    auto newer_part = makePartWithCsn(h, /*block_number=*/2, /*rows=*/10, /*creation_csn=*/1);

    RangesInDataParts parts_with_ranges;
    {
        RangesInDataPart r(boundary_part);
        r.ranges = MarkRanges{MarkRange{0, 1}};
        parts_with_ranges.push_back(std::move(r));
    }
    {
        RangesInDataPart r(newer_part);
        r.ranges = MarkRanges{MarkRange{0, 1}};
        parts_with_ranges.push_back(std::move(r));
    }

    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    size_t sum_rows = 0;
    auto log = getLogger("ReadSnapshotCsnFilterTest");

    const std::unordered_map<String, QuerySnapshot> no_snapshots;
    auto pins = applyUniqueKeyDeleteBitmaps(no_snapshots, parts_with_ranges, log, sum_marks, sum_ranges, sum_rows);

    /// Both parts survive untouched; no partition was pinned.
    ASSERT_EQ(parts_with_ranges.size(), 2u);
    ASSERT_NE(pins, nullptr);
    EXPECT_TRUE(pins->empty());
}
