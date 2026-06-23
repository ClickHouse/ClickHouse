#pragma once

#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{
class MergeTreeIndexGranularity;
class MergeTreeData;
struct RangesInDataParts;
}

namespace DB::UniqueKeyTxn
{

/// Snapshot-consistency predicate: a reader pinned at csn C sees a part iff its
/// `creation_csn ≤ C`. `uk_meta` is `IMergeTreeDataPart::getUniqueKeyMeta()` —
/// `nullopt` for legacy / no-manifest parts, which have no `creation_csn` and
/// are always visible (treated as 0). A part NEWER than C must be excluded so
/// the read never applies C-era delete bitmaps to it; this is a per-part
/// predicate, never a `snapshot->parts` set-intersect (which would drop a part
/// a concurrent merge retired by C that an in-flight SELECT still legitimately
/// reads — data loss).
inline bool isPartVisibleAtSnapshotCsn(const std::optional<UniqueKeyPartMeta> & uk_meta, CSN pinned_csn)
{
    return !uk_meta || uk_meta->creation_csn <= pinned_csn;
}

/// Surviving mark ranges after granule-skip, plus how many granules were
/// dropped (for the ProfileEvent / debug log at the call site).
struct LiveMarkRanges
{
    MarkRanges kept;
    size_t     skipped = 0;
};

/// Granule-skip for a UNIQUE KEY read: drop every mark whose rows are all in
/// `bitmap` (`rangeCardinality(row_begin, row_end) == granule_rows`), splitting
/// a run when a middle granule is excised. Pure — the production read-path
/// granule analysis (`ReadFromMergeTree::selectRangesToRead`) and the unit
/// tests both call this, so the half-open-range / last-mark arithmetic cannot
/// drift between them.
LiveMarkRanges selectLiveMarkRanges(
    const MarkRanges & ranges,
    const MergeTreeIndexGranularity & granularity,
    const DeleteBitmap & bitmap);

/// Row-level filter: `out_filter[i] = 0` for every `offsets[i]` present in
/// `bitmap`, 1 otherwise. `offsets` is the full UInt64 `_part_offset` — no
/// narrowing above UInt32. `out_filter` is resized to `count`. Returns the
/// number of surviving (kept) rows. Pure — shared by the production row filter
/// (`MergeTreeSelectProcessor`) and the unit tests.
size_t buildDeleteBitmapFilter(
    const UInt64 * offsets,
    size_t count,
    const DeleteBitmap & bitmap,
    PaddedPODArray<UInt8> & out_filter);

/// Read-path orchestration for the UNIQUE KEY delete bitmap. For each touched
/// partition, capture one snapshot pin at its current csn; per part, pick the
/// bitmap visible at that csn, stash it (+ the pinned csn) on the range, and
/// drop fully-dead granules via `selectLiveMarkRanges`. Parts newer than the
/// snapshot csn (`!isPartVisibleAtSnapshotCsn`) are dropped entirely. Parts
/// that lose all ranges are erased. Updates `sum_marks` / `sum_ranges` /
/// `sum_rows` in place (when granules were skipped OR a part was csn-filtered)
/// and returns the per-partition pins; the caller must keep them alive for the
/// lifetime of the read so GC won't reclaim a referenced bitmap. Call only when
/// the table has a unique key and `parts_with_ranges` is non-empty.
std::shared_ptr<std::vector<QuerySnapshot>> applyUniqueKeyDeleteBitmaps(
    const MergeTreeData & data,
    RangesInDataParts & parts_with_ranges,
    LoggerPtr log,
    size_t & sum_marks,
    size_t & sum_ranges,
    size_t & sum_rows);

}
