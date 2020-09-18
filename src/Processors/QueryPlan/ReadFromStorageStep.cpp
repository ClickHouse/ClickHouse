#include <Processors/QueryPlan/ReadFromStorageStep.h>

#include <Interpreters/Context.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPipeline.h>
#include <Storages/IStorage.h>

namespace DB
{

ReadFromStorageStep::ReadFromStorageStep(
    StoragePtr storage,
    const Names & required_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processing_stage,
    size_t max_block_size,
    size_t max_streams)
{
    /// Note: we read from storage in constructor of step because we don't know real header before reading.
    /// It will be fixed when storage return QueryPlanStep itself.

    Pipe pipe = storage->read(required_columns, metadata_snapshot, query_info, context, processing_stage, max_block_size, max_streams);

    if (pipe.empty())
    {
        pipe = Pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(required_columns, storage->getVirtuals(), storage->getStorageID())));

        if (query_info.prewhere_info)
        {
            if (query_info.prewhere_info->alias_actions)
            {
                pipe.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<ExpressionTransform>(header, query_info.prewhere_info->alias_actions);
                });
            }

            pipe.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FilterTransform>(
                    header,
                    query_info.prewhere_info->prewhere_actions,
                    query_info.prewhere_info->prewhere_column_name,
                    query_info.prewhere_info->remove_prewhere_column);
            });

            // To remove additional columns
            // In some cases, we did not read any marks so that the pipeline.streams is empty
            // Thus, some columns in prewhere are not removed as expected
            // This leads to mismatched header in distributed table
            if (query_info.prewhere_info->remove_columns_actions)
            {
                pipe.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<ExpressionTransform>(
                            header, query_info.prewhere_info->remove_columns_actions);
                });
            }
        }
    }

    pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    pipeline->init(std::move(pipe));

    processors = collector.detachProcessors();

    output_stream = DataStream{.header = pipeline->getHeader(), .has_single_port = pipeline->getNumStreams() == 1};
}

ReadFromStorageStep::~ReadFromStorageStep() = default;

QueryPipelinePtr ReadFromStorageStep::updatePipeline(QueryPipelines)
{
    return std::move(pipeline);
}

void ReadFromStorageStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
