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
    TableLockHolder table_lock,
    StorageMetadataPtr metadata_snapshot,
    StreamLocalLimits & limits,
    SizeLimits & leaf_limits,
    std::shared_ptr<const EnabledQuota> quota,
    StoragePtr storage,
    const Names & required_columns,
    const SelectQueryInfo & query_info,
    std::shared_ptr<Context> context,
    QueryProcessingStage::Enum processing_stage,
    size_t max_block_size,
    size_t max_streams)
{
    /// Note: we read from storage in constructor of step because we don't know real header before reading.
    /// It will be fixed when storage return QueryPlanStep itself.

    Pipe pipe = storage->read(required_columns, metadata_snapshot, query_info, *context, processing_stage, max_block_size, max_streams);

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

    pipe.setLimits(limits);

    /**
      * Leaf size limits should be applied only for local processing of distributed queries.
      * Such limits allow to control the read stage on leaf nodes and exclude the merging stage.
      * Consider the case when distributed query needs to read from multiple shards. Then leaf
      * limits will be applied on the shards only (including the root node) but will be ignored
      * on the results merging stage.
      */
    if (!storage->isRemote())
        pipe.setLeafLimits(leaf_limits);

    if (quota)
        pipe.setQuota(quota);

    pipeline->init(std::move(pipe));

    /// Add resources to pipeline. The order is important.
    /// Add in reverse order of destruction. Pipeline will be destroyed at the end in case of exception.
    pipeline->addInterpreterContext(std::move(context));
    pipeline->addStorageHolder(std::move(storage));
    pipeline->addTableLock(std::move(table_lock));

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
