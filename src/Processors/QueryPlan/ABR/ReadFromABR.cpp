#include <Processors/QueryPlan/ABR/ReadFromABR.h>

#include <atomic>

#include <Analyzer/QueryNode.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Settings.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/IReadableWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/ABR/NativeBufferingFormat.h>
#include <Processors/QueryPlan/ABR/ReplaySource.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 abr_memory_buffer_size;
    extern const SettingsBool extremes;
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

ReadFromABR::ReadFromABR(
    const SelectQueryInfo & query_info_,
    std::vector<StorageABR::SourceTable> tables_,
    StorageABR::Attempt initial_attempt_,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage_,
    LoggerPtr logger_)
    : ISourceStep(initial_attempt_.header)
    , query_info(query_info_)
    , tables(std::move(tables_))
    , initial_attempt(std::move(initial_attempt_))
    , context(std::move(context_))
    , processed_stage(processed_stage_)
    , logger(std::move(logger_))
{
}

ReadFromABR::BufferedResult ReadFromABR::executeAndBuffer(
    QueryPipeline & pipeline, const ContextPtr & context, QueryProcessingStage::Enum processed_stage)
{
    const auto & settings = context->getSettingsRef();
    const size_t mem_buffer_size = settings[Setting::abr_memory_buffer_size];
    const SharedHeader header = pipeline.getSharedHeader();

    CascadeWriteBuffer::WriteBufferPtrs prepared;
    if (mem_buffer_size > 0)
        prepared.emplace_back(std::make_shared<MemoryWriteBuffer>(mem_buffer_size));

    CascadeWriteBuffer::WriteBufferConstructors lazy;
    auto disk_scope = context->getTempDataOnDisk();
    lazy.emplace_back(
        [disk_scope](const WriteBufferPtr &) -> WriteBufferPtr
        {
            return std::make_shared<TemporaryDataBuffer>(disk_scope);
        });

    auto cascade_buf = std::make_shared<CascadeWriteBuffer>(std::move(prepared), std::move(lazy));

    const bool add_aggregation_info = processed_stage != QueryProcessingStage::Complete;
    auto format = std::make_shared<NativeBufferingFormat>(header, *cascade_buf);
    pipeline.complete(format);

    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setProcessListElement(context->getProcessListElement());
    /// An attempt that fails with a fallback error is abandoned, so the data it read or planned to read
    /// must not be counted against the read limits of the next attempt.
    pipeline.checkReadLimitsOnOwnProgress();

    /// The executor enforces `max_execution_time` of this inner query: the attempt is cancelled when the time is up (`TIMEOUT_EXCEEDED`).
    CompletedPipelineExecutor executor(pipeline);
    std::atomic<bool> timed_out = false;
    const Int64 time_limit_us = settings[Setting::max_execution_time].totalMicroseconds();
    BackgroundSchedulePoolTaskHolder deadline_task;

    std::exception_ptr exception;
    try
    {
        executor.initialize();
        if (time_limit_us > 0)
        {
            deadline_task = context->getSchedulePool()->createTask(StorageID::createEmpty(), "ABRAttemptDeadline", [&]
            {
                timed_out = true;
                executor.cancel();
            });
            deadline_task->scheduleAfter(static_cast<size_t>((time_limit_us + 999) / 1000));
        }
        executor.execute();
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    if (deadline_task)
        deadline_task->deactivate();

    if (timed_out || exception)
    {
        cascade_buf->cancel();
        if (timed_out)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "Timeout exceeded: the attempt took longer than its share of max_execution_time ({:.3f} s)",
                static_cast<double>(time_limit_us) / 1'000'000);
        std::rethrow_exception(exception);
    }

    /// Flushes the data of the last buffer.
    cascade_buf->finalize();

    ConcatReadBuffer::Buffers read_buffers;
    CascadeWriteBuffer::WriteBufferPtrs write_buffers = cascade_buf->getResultBuffers();
    for (const auto & write_buf : write_buffers)
    {
        if (!write_buf)
            continue;

        if (auto * temporary_buf = dynamic_cast<TemporaryDataBuffer *>(write_buf.get()))
        {
            if (auto read_buf = temporary_buf->read())
                read_buffers.emplace_back(std::move(read_buf));
        }
        else if (auto * readable = dynamic_cast<IReadableWriteBuffer *>(write_buf.get()))
        {
            if (auto read_buf = readable->tryGetReadBuffer())
                read_buffers.emplace_back(std::move(read_buf));
        }
    }

    /// Also without blocks, so that `rows_before_limit_at_least` and `rows_before_aggregation` of the attempt are reported.
    std::unique_ptr<ReadBuffer> read_buffer;
    if (!read_buffers.empty())
        read_buffer = std::make_unique<ConcatReadBuffer>(std::move(read_buffers));
    auto replay_source = std::make_shared<ReplaySource>(
        header,
        std::move(write_buffers),
        std::move(read_buffer),
        add_aggregation_info,
        ReplaySource::RowsBeforeSteps{.limit = format->rows_before_limit, .aggregation = format->rows_before_aggregation});

    return {
        .replay_source = std::move(replay_source),
        .totals = std::move(format->totals),
        .extremes = std::move(format->extremes)};
}

void ReadFromABR::addTotalsAndExtremes(Pipe & pipe, const BufferedResult & result)
{
    /// Do not enable auto_progress, as the rows were already accounted for in the pipeline's attempt.
    if (!result.totals.empty())
        pipe.addTotalsSource(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(result.totals), false));

    if (!result.extremes.empty())
        pipe.addExtremesSource(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(result.extremes), false));
}

void ReadFromABR::addConvertingActions(Pipe & pipe, const Block & header, const ContextPtr & context)
{
    if (blocksHaveEqualStructure(pipe.getHeader(), header))
        return;

    /// Convert header structure to expected.
    /// Also we ignore constants from result and replace it with constants from header.
    /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
    auto convert_actions = std::make_shared<ExpressionActions>(ActionsDAG::makeConvertingActions(
        pipe.getHeader().getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context,
        /* ignore_constant_values */ true));

    pipe.addSimpleTransform([&](const SharedHeader & cur_header, Pipe::StreamType) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(cur_header, convert_actions);
    });
}

void ReadFromABR::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    bool add_totals = false;
    bool add_extremes = false;
    if (processed_stage == QueryProcessingStage::Complete)
    {
        if (query_info.query_tree)
            add_totals = query_info.query_tree->as<const QueryNode &>().isGroupByWithTotals();
        else
            add_totals = query_info.query->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef()[Setting::extremes];
    }

    /// This creator runs later, from DelayedSource::work() at execution time
    /// ReadFromABR step (the object) has been destroyed at that point so we initialize the needed variables here
    auto create_lazy_stream = [
        header = output_header,
        ls_query_info = query_info,
        ls_tables = tables,
        ls_initial_attempt = initial_attempt,
        ls_context = context,
        ls_processed_stage = processed_stage,
        log = logger.load()]() mutable -> QueryPipelineBuilder
    {
        Stopwatch ladder_watch;
        auto abr_info = ls_context->getABRQueryInfoPtr();

        for (size_t i = 0; i < ls_tables.size(); ++i)
        {
            const UInt64 interval = ls_tables[i].interval;
            const auto & storage_id = ls_tables[i].storage->getStorageID();
            LOG_DEBUG(log, "Attempt {}/{}: executing on {}", i + 1, ls_tables.size(), storage_id.getNameForLogs());
            try
            {
                StorageABR::Attempt attempt;
                if (i == 0 && ls_initial_attempt.interpreter)
                {
                    /// Reuse the interpreter already built in StorageABR::read() so we
                    /// don't repeat that analysis pass on the first attempt.
                    attempt = std::move(ls_initial_attempt);
                }
                else
                {
                    attempt = StorageABR::buildAttempt(
                        ls_query_info,
                        ls_tables[i].storage,
                        ls_context,
                        /* tables_left */ ls_tables.size() - i,
                        /* total_tables */ ls_tables.size(),
                        ls_processed_stage);
                }
                BlockIO block_io = attempt.interpreter->execute();

                /// The inner pipeline must run to completion to confirm no fallback error is thrown,
                /// so results are always buffered and replayed, even on the non-retry path.
                BufferedResult result = executeAndBuffer(block_io.pipeline, attempt.context, ls_processed_stage);

                Pipe pipe(std::move(result.replay_source));
                addConvertingActions(pipe, *header, ls_context);
                addTotalsAndExtremes(pipe, result);

                LOG_DEBUG(log, "Query succeeded on {}", storage_id.getNameForLogs());

                if (abr_info)
                {
                    abr_info->duration_ms.fetch_add(ladder_watch.elapsedMilliseconds());
                    abr_info->interval.store(interval);
                }

                QueryPipelineBuilder builder;
                builder.init(std::move(pipe));
                return builder;
            }
            catch (Exception & e)
            {
                if (StorageABR::isFallbackError(e.code()) && i + 1 < ls_tables.size())
                {
                    if (abr_info)
                    {
                        std::lock_guard lock(abr_info->mutex);
                        abr_info->exception_codes[interval] = e.code();
                    }
                    LOG_WARNING(log, "Fallback from {}: {}", storage_id.getNameForLogs(), e.message());
                    continue;
                }

                /// Record ABR metrics before re-throwing so the query_log row is
                /// consistent (duration_ms > 0 will not be paired with zeros).
                /// Two cases reach here:
                ///   (a) last table threw a fallback error — all candidates exhausted;
                ///   (b) any table threw a non-fallback error — immediate rethrow.
                if (abr_info)
                {
                    abr_info->duration_ms.fetch_add(ladder_watch.elapsedMilliseconds());
                    abr_info->interval.store(interval);
                    if (StorageABR::isFallbackError(e.code()))
                    {
                        std::lock_guard lock(abr_info->mutex);
                        abr_info->exception_codes[interval] = e.code();
                    }
                }

                if (i + 1 == ls_tables.size())
                    e.addMessage("all candidate tables exhausted");

                e.addMessage("while executing query on source table {}", storage_id.getNameForLogs());
                throw;
            }
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No source tables to read from");
    };

    /// Take the same approach as ReadFromRemote of wrapping the attempts in a
    /// DelayedSource so the inner query executes at execution time rather than
    /// here at build time. This matters because we need to send Progress Packets
    /// to the root executor, and that can only happen at execution time
    pipeline.init(createDelayedPipe(output_header, std::move(create_lazy_stream), add_totals, add_extremes));
}

}
