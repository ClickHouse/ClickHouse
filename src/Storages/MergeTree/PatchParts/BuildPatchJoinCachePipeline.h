#pragma once
#include <Processors/IProcessor.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

/// Build a pipeline that reads patch parts and fills a PatchJoinCache.
/// Returns the processors to be executed by the caller via PipelineExecutor.
/// Returns nullptr if there are no Join-mode patches to process.
std::shared_ptr<Processors> buildPatchJoinCachePipeline(
    PatchJoinCachePtr patch_join_cache,
    const RangesInPatchParts & ranges_in_patch_parts,
    const std::vector<MergeTreeReadTaskInfoPtr> & per_part_infos,
    const RangesInDataParts & parts_ranges,
    const MergeTreeReadTask::Extras & extras,
    const MergeTreeReaderSettings & reader_settings,
    size_t num_buckets,
    size_t num_threads);

}
