#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace DB
{

struct PatchToApply
{
    PaddedPODArray<UInt64> result_indices;
    PaddedPODArray<UInt64> patch_indices;
    Block patch_block;

    bool empty() const
    {
        chassert(result_indices.size() == patch_indices.size());
        return result_indices.empty();
    }

    size_t rows() const
    {
        chassert(result_indices.size() == patch_indices.size());
        return result_indices.size();
    }
};

using PatchToApplyPtr = std::shared_ptr<const PatchToApply>;
using PatchesToApply = std::vector<PatchToApplyPtr>;

struct PatchSharedData
{
    virtual ~PatchSharedData() = default;
};

using PatchSharedDataPtr = std::shared_ptr<const PatchSharedData>;

struct PatchMergeSharedData : public PatchSharedData
{
};

using PatchHashMap = HashMap<UInt128, UInt64, UInt128HashCRC32>;

struct PatchJoinSharedData : public PatchSharedData
{
    PatchHashMap hash_map;
    UInt64 min_block = 0;
    UInt64 max_block = 0;
};

/// Builds a hash table from patch_block.
/// It stores a mapping (_block_number, _block_offset) -> number of row in block.
std::shared_ptr<PatchJoinSharedData> buildPatchJoinData(const Block & patch_block);

/// Applies patch. Returns indices in result and patch blocks for rows that should be updated.
PatchToApplyPtr applyPatchMerge(const Block & result_block, const Block & patch_block, const PatchPartInfoForReader & patch);
PatchToApplyPtr applyPatchJoin(const Block & result_block, const Block & patch_block, const PatchJoinSharedData & join_data);

/// Updates rows in result_block from patch_block at specified indices.
/// versions_block is a shared block with current versions of rows for each updated column.
void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const PatchesToApply & patches,
    UInt64 source_data_version);

}
