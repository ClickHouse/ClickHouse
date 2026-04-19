#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace DB
{

struct KeyDescription;

/// Represents a patch that can be applied to the result block to update the data.
struct PatchToApply
{
    /// Blocks with data from patch parts.
    std::vector<Block> patch_blocks;
    /// Index of row to update in the result block.
    PaddedPODArray<UInt64> result_row_indices;
    /// Index of patch block to take the updated row from.
    PaddedPODArray<UInt64> patch_block_indices;
    /// Index of row in patch block to take the updated row from.
    PaddedPODArray<UInt64> patch_row_indices;

    bool empty() const { return patch_blocks.empty(); }
    size_t getNumSources() const { return patch_blocks.size(); }

    size_t getNumRows() const
    {
        chassert(result_row_indices.size() == patch_row_indices.size());
        return result_row_indices.size();
    }
};

using PatchToApplyPtr = std::shared_ptr<const PatchToApply>;
using PatchesToApply = std::vector<PatchToApplyPtr>;

struct PatchReadResult
{
    virtual ~PatchReadResult() = default;
    virtual bool empty() const = 0;
};

using PatchReadResultPtr = std::shared_ptr<const PatchReadResult>;

struct PatchMergeReadResult : public PatchReadResult
{
    Block block;
    UInt64 min_part_offset = 0;
    UInt64 max_part_offset = 0;

    bool empty() const override { return block.rows() == 0; }
};

struct PatchJoinReadResult : public PatchReadResult
{
    PatchJoinCache::Entries entries;

    bool empty() const override { return entries.empty(); }
};

/// v2 patch-read result. Instead of a scalar `_part_offset` range, carries a single-row block with
/// the first and last rows' sort-key columns. Those two rows define the tuple range
/// `[min_sort_key, max_sort_key]` that `MergeTreePatchReaderMergeOnKey::needNewPatch`/`needOldPatch`
/// compare against the main-side read result's own min/max sort-key tuple.
struct PatchMergeOnKeyReadResult : public PatchReadResult
{
    Block block;
    /// A 1-row block containing the first row of `block` projected onto the sort-key columns.
    Block min_sort_key_row;
    /// Similarly for the last row.
    Block max_sort_key_row;

    bool empty() const override { return block.rows() == 0; }
};

/// Build a `PatchSortKey` from the v2 patch's rebuilt `KeyDescription` and its persisted prefix
/// length (see `SourcePartsSetForPatch::getSortKeyPrefixSize`). Slices the result/reverse arrays
/// to `prefix_size` (dropping the trailing `_block_number` / `_block_offset` identity columns) and
/// trims those two names out of the source-column set. Typically called once per patch at
/// `PatchPartInfo` construction; the result is stashed on the info struct and re-used by every
/// reader / apply / planning consumer.
PatchSortKey makePatchSortKey(const KeyDescription & sort_key, UInt64 prefix_size);

/// Applies patch. Returns indices in result and patch blocks for rows that should be updated.
PatchToApplyPtr applyPatchMerge(const Block & result_block, const Block & patch_block, const PatchPartInfoForReader & patch);
PatchToApplyPtr applyPatchJoin(const Block & result_block, const PatchJoinCache::Entry & join_entry);

/// Applies a v2 (MergeOnKey) patch. Two-cursor merge on the main table's sort-key columns; within
/// each equal-sort-key run, uses `(_block_number, _block_offset)` to identify which main-side row
/// matches which patch-side row. Memory bounded by the largest equal-sort-key run (usually 1).
PatchToApplyPtr applyPatchMergeOnKey(const Block & result_block, const Block & patch_block, const PatchSortKey & sort_key);

/// Updates rows in result_block from patch_block at specified indices.
/// versions_block is a shared block with current versions of rows for each updated column.
void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    const Names & updated_columns,
    UInt64 source_data_version);

}
