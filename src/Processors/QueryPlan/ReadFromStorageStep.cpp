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
    TableLockHolder table_lock_,
    StorageMetadataPtr metadata_snapshot_,
    StreamLocalLimits & limits_,
    std::shared_ptr<const EnabledQuota> quota_,
    StoragePtr storage_,
    const Names & required_columns_,
    const SelectQueryInfo & query_info_,
    std::shared_ptr<Context> context_,
    QueryProcessingStage::Enum processing_stage_,
    size_t max_block_size_,
    size_t max_streams_)
    : table_lock(std::move(table_lock_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , limits(limits_)
    , quota(std::move(quota_))
    , storage(std::move(storage_))
    , required_columns(required_columns_)
    , query_info(query_info_)
    , context(std::move(context_))
    , processing_stage(processing_stage_)
    , max_block_size(max_block_size_)
    , max_streams(max_streams_)
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

    /// Table lock is stored inside pipeline here.
    pipeline->addTableLock(table_lock);

    pipe.setLimits(limits);

    if (quota)
        pipe.setQuota(quota);

    pipeline->init(std::move(pipe));

    pipeline->addInterpreterContext(std::move(context));
    pipeline->addStorageHolder(std::move(storage));

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
