#pragma once

#include <Core/SortDescription.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Adds the FINAL merge transform for the table's merging engine on top of `pipe`, collapsing rows
/// that share the sort key into the single FINAL result. The pipe's input streams must already be
/// sorted by `sort_description`. For `Replacing` with `enable_vertical_final`, appends the
/// `SelectByIndicesTransform` that materializes the vertical-final result.
void addMergingFinal(
    Pipe & pipe,
    const SortDescription & sort_description,
    MergeTreeData::MergingParams merging_params,
    const StorageMetadataPtr & metadata_snapshot,
    size_t max_block_size_rows,
    bool enable_vertical_final);

}
