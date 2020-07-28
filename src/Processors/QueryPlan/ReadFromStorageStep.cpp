#include <Processors/QueryPlan/ReadFromStorageStep.h>

#include <Interpreters/Context.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPipeline.h>
#include <Storages/IStorage.h>
#include <Processors/Transforms/ConvertingTransform.h>

namespace DB
{

ReadFromStorageStep::ReadFromStorageStep(
    TableLockHolder table_lock_,
    StorageMetadataPtr & metadata_snapshot_,
    SelectQueryOptions options_,
    StoragePtr storage_,
    const Names & required_columns_,
    const SelectQueryInfo & query_info_,
    std::shared_ptr<Context> context_,
    QueryProcessingStage::Enum processing_stage_,
    size_t max_block_size_,
    size_t max_streams_)
    : table_lock(std::move(table_lock_))
    , metadata_snapshot(metadata_snapshot_)
    , options(std::move(options_))
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

    Pipes pipes = storage->read(required_columns, metadata_snapshot, query_info, *context, processing_stage, max_block_size, max_streams);

    if (pipes.empty())
    {
        Pipe pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(required_columns, storage->getVirtuals(), storage->getStorageID())));

        if (query_info.prewhere_info)
        {
            if (query_info.prewhere_info->alias_actions)
                pipe.addSimpleTransform(std::make_shared<ExpressionTransform>(
                        pipe.getHeader(), query_info.prewhere_info->alias_actions));

            pipe.addSimpleTransform(std::make_shared<FilterTransform>(
                    pipe.getHeader(),
                    query_info.prewhere_info->prewhere_actions,
                    query_info.prewhere_info->prewhere_column_name,
                    query_info.prewhere_info->remove_prewhere_column));

            // To remove additional columns
            // In some cases, we did not read any marks so that the pipeline.streams is empty
            // Thus, some columns in prewhere are not removed as expected
            // This leads to mismatched header in distributed table
            if (query_info.prewhere_info->remove_columns_actions)
                pipe.addSimpleTransform(std::make_shared<ExpressionTransform>(
                        pipe.getHeader(), query_info.prewhere_info->remove_columns_actions));
        }

        pipes.emplace_back(std::move(pipe));
    }

    pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    /// Table lock is stored inside pipeline here.
    pipeline->addTableLock(table_lock);

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

        for (auto & pipe : pipes)
        {
            if (!options.ignore_limits)
                pipe.setLimits(limits);

            if (!options.ignore_quota && (options.to_stage == QueryProcessingStage::Complete))
                pipe.setQuota(quota);
        }
    }

    for (auto & pipe : pipes)
        pipe.enableQuota();

    pipeline->init(std::move(pipes));

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
