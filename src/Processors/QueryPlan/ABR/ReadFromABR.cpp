#include <Processors/QueryPlan/ABR/ReadFromABR.h>

#include <Analyzer/QueryNode.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
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
#include <Processors/Sources/NullSource.h>
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
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadFromABR::ReadFromABR(
    const SelectQueryInfo & query_info_,
    std::span<const StoragePtr> tables_,
    std::span<const UInt64> sample_intervals_,
    size_t initial_table_idx_,
    StorageABR::Attempt initial_attempt_,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage_,
    LoggerPtr logger_)
    : ISourceStep(std::move(initial_attempt_.header))
    , query_info(query_info_)
    , tables(tables_)
    , sample_intervals(sample_intervals_)
    , initial_table_idx(initial_table_idx_)
    , initial_interpreter(std::move(initial_attempt_.interpreter))
    , context(std::move(context_))
    , processed_stage(processed_stage_)
    , logger(std::move(logger_))
{
}

ReadFromABR::BufferedResult ReadFromABR::executeAndBuffer(QueryPipeline & pipeline, const ContextPtr & context, QueryProcessingStage::Enum processed_stage)
{
    const size_t mem_buffer_size = context->getSettingsRef()[Setting::abr_memory_buffer_size];
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

    try
    {
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
    }
    catch (...)
    {
        cascade_buf->cancel();
        throw;
    }

    /// Flushes the data of the last buffer.
    cascade_buf->finalize();

    ConcatReadBuffer::Buffers read_buffers;
    for (auto & write_buf : cascade_buf->getResultBuffers())
    {
        if (!write_buf)
            continue;

        if (auto * temporary_buf = dynamic_cast<TemporaryDataBuffer *>(write_buf.get()))
        {
            if (auto reread_buf = temporary_buf->read())
                read_buffers.emplace_back(std::move(reread_buf));
        }
        else if (auto * readable = dynamic_cast<IReadableWriteBuffer *>(write_buf.get()))
        {
            if (auto reread_buf = readable->tryGetReadBuffer())
                read_buffers.emplace_back(std::move(reread_buf));
        }
    }

    std::shared_ptr<ISource> replay_source;
    if (read_buffers.empty())
        replay_source = std::make_shared<NullSource>(header);
    else
        replay_source = std::make_shared<ReplaySource>(
            header, std::make_unique<ConcatReadBuffer>(std::move(read_buffers)), add_aggregation_info);

    return {
        .replay_source = std::move(replay_source),
        .totals = std::move(format->totals),
        .extremes = std::move(format->extremes)};
}

void ReadFromABR::addTotalsAndExtremes(Pipe & pipe, const BufferedResult & result)
{
    if (!result.totals.empty())
        pipe.addTotalsSource(std::make_shared<SourceFromSingleChunk>(
            std::make_shared<const Block>(result.totals.cloneEmpty()),
            Chunk(result.totals.getColumns(), result.totals.rows())));

    if (!result.extremes.empty())
        pipe.addExtremesSource(std::make_shared<SourceFromSingleChunk>(
            std::make_shared<const Block>(result.extremes.cloneEmpty()),
            Chunk(result.extremes.getColumns(), result.extremes.rows())));
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
        ls_tables = std::vector<StoragePtr>(tables.begin(), tables.end()),
        ls_sample_intervals = std::vector<UInt64>(sample_intervals.begin(), sample_intervals.end()),
        ls_initial_table_idx = initial_table_idx,
        ls_initial_interpreter = initial_interpreter,
        ls_context = context,
        ls_processed_stage = processed_stage,
        log = logger.load()]() mutable -> QueryPipelineBuilder
    {
        Stopwatch ladder_watch;
        auto abr_info = ls_context->getABRQueryInfoPtr();

        for (size_t i = ls_initial_table_idx; i < ls_tables.size(); ++i)
        {
            const auto & storage_id = ls_tables[i]->getStorageID();
            LOG_DEBUG(log, "Attempt {}/{}: executing on {}",
                i - ls_initial_table_idx + 1,
                ls_tables.size() - ls_initial_table_idx,
                storage_id.getNameForLogs());
            try
            {
                BlockIO block_io;
                if (i == ls_initial_table_idx && ls_initial_interpreter)
                {
                    /// Reuse the interpreter already built in StorageABR::read() so we
                    /// don't repeat that analysis pass on the first attempt.
                    block_io = ls_initial_interpreter->execute();
                    ls_initial_interpreter.reset();
                }
                else
                {
                    auto attempt = StorageABR::buildAttempt(
                        ls_query_info,
                        ls_tables[i],
                        ls_context,
                        /* tables_left */ ls_tables.size() - i,
                        /* total_tables */ ls_tables.size() - ls_initial_table_idx,
                        ls_processed_stage);
                    block_io = attempt.interpreter->execute();
                }

                /// The inner pipeline must run to completion to confirm no fallback error is thrown,
                /// so results are always buffered and replayed, even on the non-retry path.
                BufferedResult result = executeAndBuffer(block_io.pipeline, ls_context, ls_processed_stage);

                Pipe pipe(std::move(result.replay_source));
                addConvertingActions(pipe, *header, ls_context);
                addTotalsAndExtremes(pipe, result);

                LOG_DEBUG(log, "Query succeeded on {}", storage_id.getNameForLogs());

                if (abr_info)
                {
                    abr_info->duration_ms.fetch_add(ladder_watch.elapsedMilliseconds());
                    abr_info->interval.store(ls_sample_intervals[i]);
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
                        abr_info->exception_codes[ls_sample_intervals[i]] = e.code();
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
                    abr_info->interval.store(ls_sample_intervals[i]);
                    if (StorageABR::isFallbackError(e.code()))
                    {
                        std::lock_guard lock(abr_info->mutex);
                        abr_info->exception_codes[ls_sample_intervals[i]] = e.code();
                    }
                }

                if (i + 1 == ls_tables.size())
                    e.addMessage("all candidate tables exhausted");

                e.addMessage("while executing query on underlying table {}", storage_id.getNameForLogs());
                throw;
            }
        }
        /// This only happens if ls_initial_table_idx >= ls_tables.size(), which should never happen
        throw Exception(ErrorCodes::LOGICAL_ERROR, "initial table index is not a valid index");
    };

    /// Take the same approach as ReadFromRemote of wrapping the attempts in a
    /// DelayedSource so the inner query executes at execution time rather than
    /// here at build time. This matters because we need to send Progress Packets
    /// to the root executor, and that can only happen at execution time
    pipeline.init(createDelayedPipe(output_header, std::move(create_lazy_stream), add_totals, add_extremes));
}

}
