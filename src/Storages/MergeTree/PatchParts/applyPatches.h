#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <absl/container/flat_hash_map.h>

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

/// We use two-level hash map (_block_number -> (_block_offset -> row_number)).
/// Block number are usually the same for the large ranges of consecutive rows.
/// Therefore we switch between hash maps for blocks rarely.
/// It makes two-level hash map more cache-friendly than single-level ((_block_number, _block_offset) -> row_number).
using OffsetsHashMap = absl::flat_hash_map<UInt64, UInt64, HashCRC32<UInt64>>;
using PatchHashMap = absl::flat_hash_map<UInt64, OffsetsHashMap, DefaultHash<UInt64>>;

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
