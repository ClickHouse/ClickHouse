#pragma once

#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>
#include <Common/Logger.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{
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
