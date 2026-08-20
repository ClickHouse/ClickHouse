#include <Processors/Transforms/AsyncInsertQueueTransform.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/arithmeticOverflow.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

/// Poll granularity for the cancellation check while waiting for the flush; `wait_for` returns
/// immediately once the future is ready, so this adds no latency.
constexpr auto flush_wait_cancellation_check_interval = std::chrono::milliseconds(100);

/// Saturating, so a pathological block cannot wrap the estimate below the real size.
void addSaturating(size_t & total, size_t value)
{
    if (common::addOverflow(total, value, total))
        total = std::numeric_limits<size_t>::max();
}

/// Mirrors what `removeSpecialRepresentations` expands, so an oversized chunk is rejected without
/// paying for the expansion first.
size_t estimateMaterializedBytes(const ColumnPtr & column, size_t rows)
{
    if (const auto * replicated = typeid_cast<const ColumnReplicated *>(column.get()))
    {
        /// Weight each nested row by its reference count; an average would undershoot a row referenced many times.
        const auto & nested = replicated->getNestedColumn();
        ColumnUInt64::Container reference_counts(nested->size(), 0);
        replicated->getIndexes().countRowsInIndexedData(reference_counts);

        size_t total = 0;
        for (size_t i = 0; i < reference_counts.size(); ++i)
        {
            size_t row_bytes = 0;
            if (common::mulOverflow(static_cast<size_t>(reference_counts[i]), nested->byteSizeAt(i), row_bytes))
                return std::numeric_limits<size_t>::max();
            addSaturating(total, row_bytes);
        }
        return total;
    }

    if (const auto * tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        size_t total = 0;
        for (const auto & element : tuple->getColumns())
            addSaturating(total, estimateMaterializedBytes(element, rows));
        return total;
    }

    /// Only the per-row cost scales; adding `byteSize` too would count the one stored value twice.
    if (column->isConst())
    {
        size_t total = 0;
        if (common::mulOverflow(column->byteSizeAt(0), rows, total))
            return std::numeric_limits<size_t>::max();
        return total;
    }

    /// `byteSize` prices the stored non-default values; `byteSizeAt(0) * rows` prices the expanded defaults.
    if (column->isSparse())
    {
        size_t default_rows_bytes = 0;
        if (common::mulOverflow(column->byteSizeAt(0), rows, default_rows_bytes))
            return std::numeric_limits<size_t>::max();
        size_t total = column->byteSize();
        addSaturating(total, default_rows_bytes);
        return total;
    }

    return column->byteSize();
}

size_t estimateMaterializedBytes(const Columns & columns, size_t rows)
{
    size_t total = 0;
    for (const auto & column : columns)
        addSaturating(total, estimateMaterializedBytes(column, rows));
    return total;
}

}

AsyncInsertQueueTransform::AsyncInsertQueueTransform(
    SharedHeader header_,
    AsynchronousInsertQueue * queue_,
    ContextMutablePtr context_,
    ASTPtr query_ast_,
    Names insert_column_names_,
    UInt64 max_data_size_,
    UInt64 wait_timeout_ms_,
    bool wait_for_flush_,
    TableLockHolder destination_lock_,
    InsertDependenciesBuilder::ConstPtr insert_dependencies_,
    StoragePtr table_,
    size_t max_threads_,
    bool no_squash_,
    bool async_insert_flag_)
    : ExceptionKeepingTransform(header_, header_, /* ignore_on_start_and_finish */ false)
    , queue(queue_)
    , context(std::move(context_))
    , query_ast(std::move(query_ast_))
    , insert_column_names(std::move(insert_column_names_))
    , max_data_size(max_data_size_)
    , wait_timeout_ms(wait_timeout_ms_)
    , wait_for_flush(wait_for_flush_)
    , destination_lock(std::move(destination_lock_))
    , logger(getLogger("AsyncInsertQueueTransform"))
    , insert_dependencies(std::move(insert_dependencies_))
    , table(std::move(table_))
    , max_threads(max_threads_)
    , no_squash(no_squash_)
    , async_insert_flag(async_insert_flag_)
{
}

AsyncInsertQueueTransform::~AsyncInsertQueueTransform()
{
    /// Safety net for a non-exceptional abort (e.g. `timeout_overflow_mode = 'break'`), the same
    /// pattern `AliasSink` uses for its own nested executor: `finish()` already made this a no-op
    /// on the normal path.
    if (fallback_executor)
    {
        try
        {
            fallback_executor->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(logger);
        }
    }
}

void AsyncInsertQueueTransform::startFallback()
{
    if (fallback_executor)
        return;

    /// Only the divert path releases it, and that path runs after the last chunk was consumed.
    chassert(insert_dependencies);

    fallback_pipeline = std::make_unique<QueryPipeline>(
        InterpreterInsertQuery::buildPushPipelineFromDependencies(insert_dependencies, context, table, max_threads, no_squash, async_insert_flag));
    fallback_pipeline->setProcessListElement(context->getProcessListElement());
    /// `report_read_progress = false`: the `SELECT` side already reported progress for these rows.
    fallback_executor = std::make_unique<PushingAsyncPipelineExecutor>(*fallback_pipeline, /* report_read_progress */ false);
    fallback_executor->start();
}

void AsyncInsertQueueTransform::disqualify(Chunk chunk)
{
    queued_eligible = false;
    held = nullptr;
    startFallback();

    /// `pending` is still in arrival order, including `*held` at its original position.
    while (!pending.empty())
    {
        fallback_executor->push(std::move(pending.front()));
        pending.pop_front();
    }
    fallback_executor->push(std::move(chunk));
}

void AsyncInsertQueueTransform::onConsume(Chunk chunk)
{
    if (!queued_eligible)
    {
        fallback_executor->push(std::move(chunk));
        return;
    }

    /// Buffered, not dropped: a zero-row chunk can still carry info an upstream transform attached
    /// (e.g. `RestoreChunkInfosTransform` after squashing). Replayed into the fallback pipeline if
    /// eligibility is lost later; simply discarded if the block is diverted instead.
    if (chunk.getNumRows() == 0)
    {
        pending.push_back(std::move(chunk));
        return;
    }

    if (!held)
    {
        const size_t estimated_bytes = estimateMaterializedBytes(chunk.getColumns(), chunk.getNumRows());
        if (estimated_bytes > max_data_size)
        {
            /// Logged apart from the reasons below: only this one rejects before the expansion.
            LOG_DEBUG(
                logger,
                "INSERT ... SELECT will be executed synchronously (reason: estimated block size {} exceeds "
                "async_insert_max_data_size {})",
                estimated_bytes, max_data_size);
            disqualify(std::move(chunk));
            return;
        }

        /// Preserve chunk info across materialization, same as the pass-through chunks above and below.
        auto chunk_infos = std::move(chunk.getChunkInfos());
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        materializeBlockInplace(block);
        Chunk materialized(block.getColumns(), block.rows());
        materialized.setChunkInfos(std::move(chunk_infos));

        /// The estimate is per value, so a column of varying-width values can still overshoot.
        if (const size_t materialized_bytes = block.bytes(); materialized_bytes > max_data_size)
        {
            LOG_DEBUG(
                logger,
                "INSERT ... SELECT will be executed synchronously (reason: materialized block size {} exceeds "
                "async_insert_max_data_size {}, estimated {})",
                materialized_bytes, max_data_size, estimated_bytes);
            disqualify(std::move(materialized));
        }
        else
        {
            pending.push_back(std::move(materialized));
            held = &pending.back();
        }
        return;
    }

    /// A second non-empty block means the result is not a single block.
    LOG_DEBUG(logger, "INSERT ... SELECT will be executed synchronously (reason: the result is not a single block)");
    disqualify(std::move(chunk));
}

bool AsyncInsertQueueTransform::canGenerate()
{
    /// Nothing ever flows out of this transform's own output port: a diverted block goes to the
    /// queue and a fallback block goes into the pipeline owned by `fallback_executor`, both from
    /// `onConsume` / `getRemaining` directly. The outer pipeline caps this port with an `EmptySink`.
    return false;
}

AsyncInsertQueueTransform::GenerateResult AsyncInsertQueueTransform::onGenerate()
{
    return {};
}

AsyncInsertQueueTransform::GenerateResult AsyncInsertQueueTransform::getRemaining()
{
    if (held)
    {
        auto block = getInputPort().getHeader().cloneWithColumns(held->detachColumns());
        held = nullptr;
        pending.clear();

        auto async_query = query_ast->clone();
        auto & async_insert_query = async_query->as<ASTInsertQuery &>();
        /// `preprocessInsertQuery` rejects an empty format and a plain `INSERT ... SELECT` carries none.
        async_insert_query.format = "Native";
        async_insert_query.columns = make_intrusive<ASTExpressionList>();
        for (const auto & name : insert_column_names)
            async_insert_query.columns->children.push_back(make_intrusive<ASTIdentifier>(name));
        auto result = queue->pushQueryWithBlock(async_query, std::move(block), context);

        /// The queue owns the block, and the flush relocks the table under a fresh query id, so it
        /// cannot join the reader group of this query: a share lock kept across the wait below lets an
        /// exclusive locker queue between the two, and neither side moves. `InsertDependenciesBuilder`
        /// holds one such lock per node of the dependency path. Only the fallback pipeline needs the
        /// builder, and it can no longer be built, so this transform is its last owner.
        chassert(insert_dependencies.use_count() == 1);
        destination_lock.reset();
        insert_dependencies.reset();

        /// The queue owns the block and the flush proceeds independently of this pipeline, so
        /// there is nothing left to keep alive here; the client gives up the flush result, i.e.
        /// written row/byte accounting and any flush error.
        if (!wait_for_flush)
            return {};

        /// `cancelQuery` sets `is_killed` before cancelling the pipeline executors, so a `KILL QUERY`
        /// (or `max_execution_time`) is visible here as `throwIfKilled()` / `checkTimeLimit()`. Shared
        /// by the loop and the post-loop check, since a `false` return under `'break'` is ignored.
        auto throw_if_killed_or_timed_out = [&]
        {
            if (auto process_list_elem = context->getProcessListElement())
            {
                process_list_elem->throwIfKilled();
                process_list_elem->checkTimeLimit();
            }
        };

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_timeout_ms);
        while (true)
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline)
                break;

            /// Clamped, so a timeout shorter than the interval is not rounded up to a multiple of it.
            const auto remaining_wait = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            if (result.future.wait_for(std::min(flush_wait_cancellation_check_interval, remaining_wait)) == std::future_status::ready)
                break;

            throw_if_killed_or_timed_out();

            if (isCancelled())
                return {};
        }

        /// Readiness must not outrank a kill or an expired `max_execution_time` observed in the same poll window.
        throw_if_killed_or_timed_out();

        if (result.future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout ({} ms) exceeded", wait_timeout_ms);

        /// The future is already ready, so the timeout argument below is unused; `report_read_progress`
        /// is false because the `SELECT` side already reported its own reads.
        waitForAsyncInsertAndReportProgress(
            result.future, /* future is already ready, unused for waiting */ 0,
            context->getProcessListElement(), context->getProgressCallback(),
            /* report_read_progress */ false);
    }

    /// Set only once the fallback pipeline actually ran (`disqualify` -> `startFallback`); a no-op
    /// otherwise, e.g. an eligible query that diverted above, or an empty `SELECT` result that never
    /// produced a block to divert or fall back at all.
    if (fallback_executor)
        fallback_executor->finish();

    return {};
}

}
