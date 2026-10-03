#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>

#include <base/types.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

class IDataPartStorage;
class IMergeTreeDataPart;
class MergedPartOffsets;
class ReadSnapshot;
struct MergeTreeDataPartChecksums;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeDataPartsVector = std::vector<MergeTreeDataPartPtr>;

/// Where each source row of a UNIQUE KEY merge landed in the merged part, translated through the
/// merge's row mapping. Borrows `merged_part_offsets`.
class UniqueKeyMergeRowMap
{
public:
    UniqueKeyMergeRowMap(
        const MergeTreeDataPartsVector & sources,
        const ReadSnapshot & read_snapshot,
        const MergedPartOffsets & merged_part_offsets);

    /// The merged row of `source_row` of source `source_index`, or nullopt if it was dead at the snapshot.
    std::optional<UInt64> toMergedRow(size_t source_index, UInt64 source_row) const;

    /// Every source row live at the snapshot, which is the merged part's row count.
    UInt64 mergedRowsCount() const { return merged_rows_count; }

private:
    /// Resolved once: `toMergedRow` runs per index entry and per late kill.
    std::vector<ConstDeleteBitmapPtr> snapshot_bitmaps;
    const MergedPartOffsets & merged_part_offsets;
    /// Each source's first merged row, used when the merge appends the sources in order.
    std::vector<UInt64> merged_start;
    UInt64 merged_rows_count = 0;
};

/// Writes the merged part's `unique_key_index.sst` as a k-way merge of the sources' SSTs.
class UniqueKeyMergedIndexBuilder
{
public:
    UniqueKeyMergedIndexBuilder(
        const MergeTreeDataPartsVector & sources,
        const UniqueKeyMergeRowMap & row_map,
        IDataPartStorage & merged_part_storage,
        ContextPtr context);

    /// Runs the merge and commits the SST like the INSERT path.
    void build(MergeTreeDataPartChecksums & out_checksums, bool fsync);

private:
    const MergeTreeDataPartsVector & sources;
    const UniqueKeyMergeRowMap & row_map;
    IDataPartStorage & merged_part_storage;
    ContextPtr context;
};

}
