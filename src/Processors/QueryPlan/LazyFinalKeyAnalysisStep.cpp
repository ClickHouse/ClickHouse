#include <Processors/Port.h>
#include <Processors/QueryPlan/LazyFinalKeyAnalysisStep.h>
#include <Processors/Transforms/LazyFinalKeyAnalysisTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

LazyFinalKeyAnalysisStep::LazyFinalKeyAnalysisStep(
    SharedHeader input_header_,
    FutureSetPtr future_set_,
    LazyFinalSharedStatePtr shared_state_,
    StorageMetadataPtr metadata_snapshot_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeSettingsPtr data_settings_,
    const MergeTreeData & data_,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
    RangesInDataPartsPtr ranges_,
    ContextPtr query_context_,
    float min_filtered_ratio_)
    : ITransformingStep(
        input_header_,
        input_header_,
        {
            .data_stream_traits = {.returns_single_stream = true, .preserves_number_of_streams = true, .preserves_sorting = false},
            .transform_traits = {.preserves_number_of_rows = false},
        })
    , future_set(std::move(future_set_))
    , shared_state(std::move(shared_state_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , data_settings(std::move(data_settings_))
    , data(data_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , ranges(std::move(ranges_))
    , query_context(std::move(query_context_))
    , min_filtered_ratio(min_filtered_ratio_)
{
}

void LazyFinalKeyAnalysisStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const SharedHeader &)
    {
        return std::make_shared<LazyFinalKeyAnalysisTransform>(
            future_set, shared_state, metadata_snapshot, mutations_snapshot,
            storage_snapshot, data_settings, data, max_block_numbers_to_read,
            ranges, query_context, min_filtered_ratio);
    });
}

std::unique_ptr<ReadFromMergeTree> LazyFinalKeyAnalysisStep::buildReadingStep() const
{
    return LazyFinalKeyAnalysisTransform::buildReadingStep(
        metadata_snapshot, mutations_snapshot, storage_snapshot,
        data_settings, data, max_block_numbers_to_read, ranges, query_context);
}

}
