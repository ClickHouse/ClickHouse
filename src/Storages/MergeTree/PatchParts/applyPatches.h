#pragma once
#include <deque>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>

namespace DB
{

/// Represents a patch that can be applied to the result block to update the data.
struct PatchToApply
{
    /// Blocks with data from patch parts.
    std::vector<ConstBlockPtr> patch_blocks;
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

using PatchReadResultPtr = std::shared_ptr<PatchReadResult>;

struct PatchMergeReadResult : public PatchReadResult
{
    struct BlockInfo
    {
        ConstBlockPtr block;
        UInt64 min_part_offset = 0;
        UInt64 max_part_offset = 0;
    };

    std::deque<BlockInfo> blocks;

    void addBlock(ConstBlockPtr blk, UInt64 min_offset, UInt64 max_offset)
    {
        blocks.push_back({std::move(blk), min_offset, max_offset});
    }

    void evict(UInt64 min_part_offset)
    {
        while (!blocks.empty() && blocks.front().max_part_offset < min_part_offset)
            blocks.pop_front();
    }

    UInt64 lastMaxPartOffset() const
    {
        return blocks.empty() ? 0 : blocks.back().max_part_offset;
    }

    bool empty() const override { return blocks.empty(); }
};

/// Applies patch. Returns indices in result and patch blocks for rows that should be updated.
PatchToApplyPtr applyPatchMerge(const Block & result_block, const ConstBlockPtr & patch_block, const PatchPartInfoForReader & patch);

/// Updates rows in result_block from patch_block at specified indices.
/// versions_block is a shared block with current versions of rows for each updated column.
void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    const Names & updated_columns,
    UInt64 source_data_version);

}
