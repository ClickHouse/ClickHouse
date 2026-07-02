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
#include <unordered_map>
#include <vector>

namespace DB
{
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
/// TODO(unique-key): ordinary INSERT parts are written via the plain
/// `MergeTreeSink` and carry no `unique_key.txt` / `creation_csn`, so they reach
/// this predicate as `uk_meta == nullopt` and are always visible — an INSERT
/// committed after a reader's pin is therefore visible to that reader (standard
/// MergeTree snapshot-at-query-start semantics; such a part also isn't in the
/// query-start part list under normal timing). Full INSERT snapshot-isolation
/// arrives when INSERT publishes through the txn layer (the UPSERT/txn-write PR).
inline bool isPartVisibleAtSnapshotCsn(const std::optional<UniqueKeyPartMeta> & uk_meta, CSN pinned_csn)
{
    return !uk_meta || uk_meta->creation_csn <= pinned_csn;
}

/// Read-path orchestration for the UNIQUE KEY delete bitmap. Takes the snapshot
/// map pinned at part-selection time (`MergeTreeData::SnapshotData::
/// uk_partition_snapshots`, keyed by partition_id). For each part, pick its
/// partition's snapshot: if present, pick the bitmap visible at that csn, stash
/// it on the range, and drop fully-dead granules via `selectLiveMarkRanges`;
/// parts newer than the snapshot csn (`!isPartVisibleAtSnapshotCsn`) are dropped
/// entirely. If the partition is ABSENT from the map (no controller existed at
/// pin time ⇒ no committed DELETE ⇒ no pre-snapshot bitmaps), the part is fully
/// live — skip the bitmap filter; do NOT lazily pin (a gap-DELETE's bitmap must
/// be ignored for this snapshot). Parts that lose all ranges are erased. Updates
/// `sum_marks` / `sum_ranges` / `sum_rows` in place (when granules were skipped
/// OR a part was csn-filtered) and returns the used pins (`.share()`d out of the
/// map); the caller must keep them alive for the lifetime of the read so GC
/// won't reclaim a referenced bitmap. Call only when the table has a unique key
/// and `parts_with_ranges` is non-empty.
std::shared_ptr<std::vector<QuerySnapshot>> applyUniqueKeyDeleteBitmaps(
    const std::unordered_map<String, QuerySnapshot> & partition_snapshots,
    RangesInDataParts & parts_with_ranges,
    LoggerPtr log,
    size_t & sum_marks,
    size_t & sum_ranges,
    size_t & sum_rows);

}
