#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace DB
{

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

/// Applies patch. Returns indices in result and patch blocks for rows that should be updated.
PatchToApplyPtr applyPatchMerge(const Block & result_block, const Block & patch_block, const PatchPartInfoForReader & patch);
PatchToApplyPtr applyPatchJoin(const Block & result_block, const PatchJoinCache::Entry & join_entry);

/// Updates rows in result_block from patch_block at specified indices.
/// versions_block is a shared block with current versions of rows for each updated column.
void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    const Names & updated_columns,
    UInt64 source_data_version);

}
