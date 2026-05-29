#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/SparseOffsetsShare.h>
#include <Storages/MergeTree/SparsityFilter.h>
#include <Common/Logger.h>

#include <atomic>

#include <optional>
#include <vector>


namespace DB
{

class MergeTreeData;
class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
struct RangesInDataPart;

/// Per-granule classification of a sparse-encoded column. The two vectors are
/// indexed by mark number and are mutually exclusive (a granule can be all-default,
/// all-non-default, or mixed).
struct SparseGranuleAnalysis
{
    std::vector<bool> granule_has_only_defaults;
    std::vector<bool> granule_has_only_non_defaults;
};

/// Read the sparse-offsets substream of `column_name` over `ranges` and classify every
/// granule. Returns `std::nullopt` when the column isn't sparse-encoded on this part
/// (there is no offsets stream to inspect, and a dense scan can't be served cheaper
/// than just running the predicate).
/// `offsets_share` (optional): when non-null, the analyzer persists the per-range
/// decompressed offsets it reads so the data scan can serve its own reads of the
/// same column without going back to disk.
std::optional<SparseGranuleAnalysis>
analyzeSparseColumnGranules(
    const DataPartPtr & part,
    const String & column_name,
    const MarkRanges & ranges,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & query_context,
    SparseOffsetsShare * offsets_share,
    LoggerPtr log,
    const std::atomic<bool> * is_cancelled = nullptr);


/// `granules_selected[g] == false` means the scan can skip granule g. Adapted from
/// `SparseGranuleAnalysis` to match the bitmap that `MergeTreeReaderIndex::canSkipMark`
/// consumes from skip-index readers.
struct SparsityReadResult
{
    std::vector<bool> granules_selected;
};
using SparsityReadResultPtr = std::shared_ptr<SparsityReadResult>;

/// Runs the per-part classification on demand from `MergeTreeIndexReadResultPool`,
/// so the analyzer's I/O happens at scan time alongside the predicate read instead
/// of during query planning. Drops a granule when any conjunct proves no row in it
/// can match (`AND` semantics).
class MergeTreeSparsityReader
{
public:
    MergeTreeSparsityReader(
        std::vector<RecognisedSparsityPredicate> predicates_,
        const MergeTreeData & data_,
        StorageSnapshotPtr storage_snapshot_,
        ContextPtr query_context_,
        LoggerPtr log_);

    SparsityReadResultPtr read(const RangesInDataPart & part);

    /// Owned by `MergeTreeIndexReadResultPool`; set after construction. When set, the
    /// analyzer writes its decompressed per-range offsets here so the data-scan path
    /// can pick them up.
    void setSparseOffsetsShare(SparseOffsetsSharePtr share) { offsets_share = std::move(share); }

    /// Marks the reader as cancelled. Outstanding `read` calls return nullptr; pending
    /// chunk work inside `analyzeSparseColumnGranules` is abandoned. Idempotent.
    void cancel() noexcept { is_cancelled.store(true, std::memory_order_release); }

private:
    std::vector<RecognisedSparsityPredicate> predicates;
    const MergeTreeData & data;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr query_context;
    SparseOffsetsSharePtr offsets_share;
    LoggerPtr log;
    std::atomic<bool> is_cancelled{false};
};

using MergeTreeSparsityReaderPtr = std::shared_ptr<MergeTreeSparsityReader>;

}
