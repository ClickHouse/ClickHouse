#pragma once

#include <Storages/MergeTree/MarkRange.h>
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

}
