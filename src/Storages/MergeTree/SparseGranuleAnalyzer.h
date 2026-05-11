#pragma once

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
class StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
struct RangesInDataPart;

/// Per-granule state for a sparse-encoded column.
struct SparseGranuleAnalysis
{
    /// `true` for granules that contain only default values (offsets stream is empty for
    /// this granule). Predicates of class `MatchesNonDefault` (e.g. `col != 0`) can drop
    /// these granules without reading values.
    std::vector<bool> granule_has_only_defaults;
    /// `true` for granules that contain no default values. Predicates of class
    /// `MatchesDefault` (e.g. `col = 0`) can drop these.
    std::vector<bool> granule_has_only_non_defaults;
};

/// Reads the sparse-offsets substream of `column_name` from `part` over `ranges` and
/// classifies every granule in those ranges. Works regardless of part format (Wide,
/// Compact, Packed) because it goes through `createMergeTreeReader` which dispatches
/// on the part type.
///
/// Returns `std::nullopt` when the column isn't sparse-encoded on this part (Phase B
/// only applies to sparse-encoded columns; dense columns are handled by Phase A or
/// by a full scan).
std::optional<SparseGranuleAnalysis>
analyzeSparseColumnGranules(
    const DataPartPtr & part,
    const String & column_name,
    const MarkRanges & ranges,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    LoggerPtr log);


/// Result of Phase B granule analysis adapted to the `granules_selected` bitmap that
/// `MergeTreeReaderIndex::canSkipMark` consumes. `granules_selected[g] == false` means
/// "skip granule g".
struct SparsityReadResult
{
    std::vector<bool> granules_selected;
};
using SparsityReadResultPtr = std::shared_ptr<SparsityReadResult>;

/// Lazy granule analyzer used by the `data_read` mode of `use_sparsity_info_for_pruning`.
/// `MergeTreeIndexReadResultPool` calls `read(part)` once per part; the resulting mask
/// is then fed into `MergeTreeReaderIndex::canSkipMark` like a skip-index result.
/// Supports multi-conjunct WHEREs: every classified `AND` conjunct contributes its
/// own granule-drop verdict; the granule is skipped iff any single conjunct proves
/// no rows in it can match.
class MergeTreeSparsityReader
{
public:
    MergeTreeSparsityReader(
        std::vector<RecognisedSparsityPredicate> predicates_,
        const MergeTreeData & data_,
        StorageSnapshotPtr storage_snapshot_,
        LoggerPtr log_);

    SparsityReadResultPtr read(const RangesInDataPart & part);

private:
    std::vector<RecognisedSparsityPredicate> predicates;
    const MergeTreeData & data;
    StorageSnapshotPtr storage_snapshot;
    LoggerPtr log;
};

using MergeTreeSparsityReaderPtr = std::shared_ptr<MergeTreeSparsityReader>;

}
