#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/SparsityFilter.h>
#include <Common/Logger.h>

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
std::optional<SparseGranuleAnalysis>
analyzeSparseColumnGranules(
    const DataPartPtr & part,
    const String & column_name,
    const MarkRanges & ranges,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & query_context,
    LoggerPtr log);


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

private:
    std::vector<RecognisedSparsityPredicate> predicates;
    const MergeTreeData & data;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr query_context;
    LoggerPtr log;
};

using MergeTreeSparsityReaderPtr = std::shared_ptr<MergeTreeSparsityReader>;

}
