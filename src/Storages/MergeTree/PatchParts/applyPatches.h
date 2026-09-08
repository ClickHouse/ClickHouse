#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace DB
{

struct KeyDescription;

/// Represents a patch that can be applied to the result block to update the data.
struct PatchIndices
{
    /// Blocks with data from patch parts.
    Blocks patch_blocks;
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

using PatchIndicesPtr = std::shared_ptr<const PatchIndices>;
using PatchesIndices = std::vector<PatchIndicesPtr>;

struct PatchReadResult
{
    virtual ~PatchReadResult() = default;
    virtual bool empty() const = 0;
};

using PatchReadResultPtr = std::shared_ptr<const PatchReadResult>;

struct PatchMergeReadResult : public PatchReadResult
{
    Block block;
    /// Offsets of the source part covered by the read range. Not set if the range has no rows of that part.
    std::optional<UInt64> min_part_offset;
    std::optional<UInt64> max_part_offset;

    bool empty() const override { return block.rows() == 0; }
};

struct PatchJoinReadResult : public PatchReadResult
{
    PatchJoinCache::Entries entries;

    bool empty() const override { return entries.empty(); }
};

/// v2 patch-read result. Sort-key result columns are materialized on `block` by the reader.
struct PatchMergeOnKeyReadResult : public PatchReadResult
{
    Block block;

    bool empty() const override { return block.rows() == 0; }
};

/// A read result of a patch part with the set of result-block columns updated from it.
struct PatchReadResultToApply
{
    PatchPartInfoForReader patch;
    PatchReadResultPtr read_result;
    Names updated_columns;
};

/// Builds patches of all modes from patch read results and applies them to result_block.
/// Patches updating the same set of columns are combined and applied together.
void applyPatchesToBlock(
    Block & result_block,
    Block & versions_block,
    const std::vector<PatchReadResultToApply> & patch_read_results,
    UInt64 source_data_version);

/// Helpers defined in applyPatches.cpp, shared with the legacy formats (applyPatchesLegacy.cpp).
const PaddedPODArray<UInt64> & getColumnUInt64Data(const Block & block, const String & column_name);
PaddedPODArray<UInt64> & getColumnUInt64Data(Block & block, const String & column_name);
bool canApplyPatchInplace(const IColumn & column);
IColumn::Versions & addDataVersionForColumn(Block & block, const String & column_name, UInt64 num_rows, UInt64 data_version);
Block getUpdatedHeader(const PatchesIndices & patches);

/// Applies each patch as-is, without combining row indices across patches.
/// Patches may have multiple source blocks (e.g. built by applyPatchesMergeOnKey).
void applyPatchesIndices(
    Block & result_block,
    Block & versions_block,
    const PatchesIndices & patches,
    const Block & updated_header,
    UInt64 source_data_version);

}
