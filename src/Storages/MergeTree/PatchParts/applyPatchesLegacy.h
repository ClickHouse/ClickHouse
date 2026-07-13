#pragma once
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Columns/IColumn.h>

namespace DB
{

/// Apply machinery for patches in the legacy v1 formats (Merge and Join modes).
/// The current format (v2, MergeOnKey) is applied in applyPatches.cpp.

/// Builds and applies patches with the legacy Merge and Join modes from patch read results.
/// Patches updating the same set of columns are combined and applied together.
void applyPatchReadResultsLegacy(
    Block & result_block,
    Block & versions_block,
    const std::vector<PatchReadResultToApply> & patch_read_results,
    UInt64 source_data_version);

/// Helpers shared with the current format, defined in applyPatches.cpp.
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
