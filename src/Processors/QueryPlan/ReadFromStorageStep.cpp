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
    StorageMetadataPtr & metadata_snapshot,
    SelectQueryOptions options,
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

    /// Set the limits and quota for reading data, the speed and time of the query.
    {
        const Settings & settings = context->getSettingsRef();

        IBlockInputStream::LocalLimits limits;
        limits.mode = IBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
        limits.speed_limits.max_execution_time = settings.max_execution_time;
        limits.timeout_overflow_mode = settings.timeout_overflow_mode;

        /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
          *  because the initiating server has a summary of the execution of the request on all servers.
          *
          * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
          *  additionally on each remote server, because these limits are checked per block of data processed,
          *  and remote servers may process way more blocks of data than are received by initiator.
          *
          * The limits to throttle maximum execution speed is also checked on all servers.
          */
        if (options.to_stage == QueryProcessingStage::Complete)
        {
            limits.speed_limits.min_execution_rps = settings.min_execution_speed;
            limits.speed_limits.min_execution_bps = settings.min_execution_speed_bytes;
        }

        limits.speed_limits.max_execution_rps = settings.max_execution_speed;
        limits.speed_limits.max_execution_bps = settings.max_execution_speed_bytes;
        limits.speed_limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;

        auto quota = context->getQuota();

        if (!options.ignore_limits)
            pipe.setLimits(limits);

        if (!options.ignore_quota && (options.to_stage == QueryProcessingStage::Complete))
            pipe.setQuota(quota);
    }

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
