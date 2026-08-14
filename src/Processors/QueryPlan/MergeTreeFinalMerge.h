#pragma once

#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <functional>
#include <optional>

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

/// One lane of a distributed task's read: the marks it reads. When `needs_merge` it is a FINAL intersecting
/// layer (read in order, trimmed to `(borders[index-1], borders[index]]`, then merge-deduplicated); otherwise
/// a plain or non-intersecting read with `borders`/`index` unused.
struct DistributedReadBucket
{
    RangesInDataPartsDescription marks;
    bool needs_merge = false;
    std::vector<std::vector<Field>> borders;
    size_t index = 0;
};

/// Reads one lane's marks into a pipe -- the part-source-specific seam of `buildDistributedFinalPipe`.
using DistributedFinalReadStepGetter = std::function<Pipe(const RangesInDataPartsDescription & marks)>;

/// Reads the non-intersecting (no-merge) lanes of a distributed FINAL, applying the engine's FINAL
/// sign/is-deleted filter (`Collapsing` hides unmatched negative-sign rows; `Replacing` with an is-deleted
/// column hides deleted rows; other engines need none). `read` reads the given columns into a pipe.
Pipe readNonIntersectingFinalWithEngineFilter(
    const MergeTreeData::MergingParams & merging_params,
    const Names & origin_column_names,
    ContextPtr context,
    const std::function<Pipe(const Names & columns)> & read);

/// Builds a single full FINAL merge over all parts: sorts the in-order streams from `read_all_parts_in_order`
/// by the sorting key and merges them in one group to remove duplicate rows, with no primary-key-range split.
/// Used when a FINAL read was not split by primary-key range (a distributed read that lands every part on one
/// node, so a full merge over them matches single-node FINAL). The primary key need not be range-splittable.
/// `out_projection` maps the merge output (which carries the extra sorting columns) back to the input columns.
Pipe buildFullFinalMergePipe(
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::MergingParams merging_params,
    size_t max_block_size_rows,
    bool enable_vertical_final,
    ContextPtr context,
    std::optional<ActionsDAG> & out_projection,
    const std::function<Pipe()> & read_all_parts_in_order);

/// Builds the FINAL read pipe for a distributed task's lanes, like single-node parallel FINAL: one in-order
/// read + PK-range-layer trim + merge-dedup per intersecting lane, plus one engine-filtered read of the
/// non-intersecting lanes, all united. `read_lane_in_order` and `read_non_intersecting` turn lane marks into
/// pipes (the part-source seam).
Pipe buildDistributedFinalPipe(
    const std::vector<DistributedReadBucket> & lanes,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::MergingParams merging_params,
    size_t max_block_size_rows,
    bool enable_vertical_final,
    ContextPtr context,
    std::optional<ActionsDAG> & out_projection,
    const DistributedFinalReadStepGetter & read_lane_in_order,
    const DistributedFinalReadStepGetter & read_non_intersecting);

}
