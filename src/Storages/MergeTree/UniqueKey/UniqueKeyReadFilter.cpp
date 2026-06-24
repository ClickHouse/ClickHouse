#include <Storages/MergeTree/UniqueKey/UniqueKeyReadFilter.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>
#include <Common/ProfileEvents.h>

#include <unordered_map>

namespace ProfileEvents
{
extern const Event UniqueKeyBitmapGranulesSkipped;
}

namespace DB::UniqueKeyTxn
{

namespace
{

/// Surviving mark ranges after granule-skip, plus how many granules were
/// dropped (for the ProfileEvent / debug log at the call site).
struct LiveMarkRanges
{
    MarkRanges kept;
    size_t     skipped = 0;
};

/// Granule-skip for a UNIQUE KEY read: drop every mark whose rows are all in
/// `bitmap` (`rangeCardinality(row_begin, row_end) == granule_rows`), splitting
/// a run when a middle granule is excised. Mirrors the production read-path
/// granule analysis (`ReadFromMergeTree::selectRangesToRead`); covered
/// end-to-end by the stateless test `04155_unique_key_delete_partitions_predicates.sql`.
LiveMarkRanges selectLiveMarkRanges(
    const MarkRanges & ranges,
    const MergeTreeIndexGranularity & granularity,
    const DeleteBitmap & bitmap)
{
    LiveMarkRanges out;
    out.kept.reserve(ranges.size());
    for (const auto & range : ranges)
    {
        MarkRange current{};
        bool current_valid = false;
        for (size_t mark = range.begin; mark < range.end; ++mark)
        {
            const UInt64 row_begin = granularity.getMarkStartingRow(mark);
            const size_t granule_rows = granularity.getMarkRows(mark);
            const UInt64 row_end = row_begin + granule_rows;
            const UInt64 dead = (granule_rows == 0) ? 0 : bitmap.rangeCardinality(row_begin, row_end);
            const bool fully_dead = granule_rows > 0 && dead == granule_rows;

            if (fully_dead)
            {
                ++out.skipped;
                if (current_valid)
                {
                    out.kept.push_back(current);
                    current_valid = false;
                }
            }
            else if (!current_valid)
            {
                current.begin = mark;
                current.end = mark + 1;
                current_valid = true;
            }
            else
            {
                current.end = mark + 1;
            }
        }
        if (current_valid)
            out.kept.push_back(current);
    }
    return out;
}

}

std::shared_ptr<std::vector<QuerySnapshot>> applyUniqueKeyDeleteBitmaps(
    const MergeTreeData & data,
    RangesInDataParts & parts_with_ranges,
    LoggerPtr log,
    size_t & sum_marks,
    size_t & sum_ranges,
    size_t & sum_rows)
{
    size_t total_granules_skipped = 0;
    size_t parts_filtered_by_csn = 0;
    size_t sum_marks_after_bitmap = 0;
    size_t sum_ranges_after_bitmap = 0;
    size_t sum_rows_after_bitmap = 0;

    auto pins = std::make_shared<std::vector<QuerySnapshot>>();
    pins->reserve(8);
    std::unordered_map<String, size_t> partition_id_to_pin;
    partition_id_to_pin.reserve(8);

    for (auto & part_with_ranges : parts_with_ranges)
    {
        const auto & data_part = part_with_ranges.data_part;
        const String & partition_id = data_part->info.getPartitionId();

        size_t pin_idx = 0;
        auto it = partition_id_to_pin.find(partition_id);
        if (it == partition_id_to_pin.end())
        {
            auto & txn_controller = data.getOrCreateTxnController(partition_id);
            pins->push_back(txn_controller.takeQuerySnapshot());
            pin_idx = pins->size() - 1;
            partition_id_to_pin.emplace(partition_id, pin_idx);
        }
        else
        {
            pin_idx = it->second;
        }

        const auto & snapshot = (*pins)[pin_idx];
        const CSN pinned_csn = snapshot.pinnedCsn();

        /// Snapshot-consistency at the pinned csn C: a reader at C sees only
        /// parts with `creation_csn ≤ C` (and, per kept part, the bitmap with
        /// max csn ≤ C). A part newer than C must not have C-era bitmaps
        /// applied to it, so drop it from this read.
        /// TODO(unique-key): two deferred refinements, both tied to features
        /// not in this PR: (a) the snapshot is pinned here, after the part list
        /// was captured upstream — once UPSERT lands, an upsert committed in
        /// that gap could pair an old part's kills with a not-yet-visible
        /// replacement part (row vanishing); the pin must move to part-list
        /// capture time. (b) merge↔bitmap reconciliation (see the DELETE-vs-
        /// merge TODO in `UniqueKeyDelete.cpp`).
        if (!isPartVisibleAtSnapshotCsn(data_part->getUniqueKeyMeta(), pinned_csn))
        {
            ++parts_filtered_by_csn;
            part_with_ranges.ranges.clear();  /// erased below with the empty-range parts
            continue;
        }

        /// `bitmap_at` unset == no UK txn state to consult, so no filter.
        /// Once present it returns a NON-NULL bitmap by contract
        /// (empty == "no deletions"); the value below is never null, so
        /// downstream branches on `->empty()` only.
        ConstDeleteBitmapPtr bitmap =
            snapshot->bitmap_at ? snapshot->bitmap_at(data_part->name) : std::make_shared<DeleteBitmap>();
        part_with_ranges.delete_bitmap = bitmap;
        part_with_ranges.pinned_bitmap_csn = pinned_csn;

        if (bitmap->empty())
        {
            sum_marks_after_bitmap += part_with_ranges.getMarksCount();
            sum_ranges_after_bitmap += part_with_ranges.ranges.size();
            sum_rows_after_bitmap += part_with_ranges.getRowsCount();
            continue;
        }

        /// Drop fully-dead granules.
        auto live = selectLiveMarkRanges(
            part_with_ranges.ranges, *data_part->index_granularity, *bitmap);
        total_granules_skipped += live.skipped;
        part_with_ranges.ranges = std::move(live.kept);

        sum_marks_after_bitmap += part_with_ranges.getMarksCount();
        sum_ranges_after_bitmap += part_with_ranges.ranges.size();
        sum_rows_after_bitmap += part_with_ranges.getRowsCount();
    }

    /// Drop parts that lost all mark ranges to granule skipping OR to the
    /// snapshot-csn filter (their ranges were cleared above).
    std::erase_if(parts_with_ranges, [](const auto & p) { return p.ranges.empty(); });

    /// Rewrite the read-stat totals when ANY part shrank — granule skipping or
    /// a csn-filtered drop. A csn-filtered part contributes nothing to the
    /// `*_after_bitmap` running sums (it `continue`s before they accumulate),
    /// so the totals already exclude it; we just need to publish them.
    if (total_granules_skipped > 0 || parts_filtered_by_csn > 0)
    {
        if (total_granules_skipped > 0)
            ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapGranulesSkipped, total_granules_skipped);
        LOG_DEBUG(log,
            "UNIQUE KEY delete bitmap: skipped {} fully-dead granules, dropped {} parts newer than snapshot; "
            "marks {} -> {}, rows {} -> {}",
            total_granules_skipped, parts_filtered_by_csn, sum_marks, sum_marks_after_bitmap, sum_rows, sum_rows_after_bitmap);
        sum_ranges = sum_ranges_after_bitmap;
        sum_marks = sum_marks_after_bitmap;
        sum_rows = sum_rows_after_bitmap;
    }

    /// The pins keep `IPinRegistry`'s csn-count >= 1 for every snapshot read
    /// against, so GC won't unlink a bitmap any
    /// `RangesInDataPart::delete_bitmap` still references.
    return pins;
}

}
