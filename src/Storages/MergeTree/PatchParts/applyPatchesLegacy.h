#pragma once
#include <Storages/MergeTree/PatchParts/applyPatches.h>

namespace DB
{

/// Apply machinery for patches in the legacy v1 formats (Merge and Join modes).
/// The current format (v2, MergeOnKey) is applied in applyPatches.cpp.

/// Builds and applies patches with the legacy Merge and Join modes from patch read results.
/// Patches updating the same set of columns are combined and applied together.
void applyPatchesToBlockLegacy(
    Block & result_block,
    Block & versions_block,
    const std::vector<PatchReadResultToApply> & patch_read_results,
    UInt64 source_data_version);

}
