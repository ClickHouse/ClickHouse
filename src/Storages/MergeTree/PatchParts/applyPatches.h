#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace DB
{

struct PatchToApply
{
    PaddedPODArray<UInt64> result_row_indices;
    PaddedPODArray<UInt64> patch_col_indices;
    PaddedPODArray<UInt64> patch_row_indices;
    std::vector<Block> patch_blocks;

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
};

using PatchReadResultPtr = std::shared_ptr<const PatchReadResult>;

struct PatchMergeReadResult : public PatchReadResult
{
    Block block;
    UInt64 min_part_offset = 0;
    UInt64 max_part_offset = 0;
};

struct PatchJoinReadResult : public PatchReadResult
{
    PatchJoinCache::EntryPtr entry;
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
    UInt64 source_data_version);

}
