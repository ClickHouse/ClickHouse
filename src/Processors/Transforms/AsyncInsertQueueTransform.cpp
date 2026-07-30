#include <Processors/Transforms/AsyncInsertQueueTransform.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>
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
    UInt64 wait_timeout_ms_)
    : ExceptionKeepingTransform(header_, header_, /* ignore_on_start_and_finish */ false)
    , queue(queue_)
    , context(std::move(context_))
    , query_ast(std::move(query_ast_))
    , insert_column_names(std::move(insert_column_names_))
    , max_data_size(max_data_size_)
    , wait_timeout_ms(wait_timeout_ms_)
    , logger(getLogger("AsyncInsertQueueTransform"))
{
}

void AsyncInsertQueueTransform::onConsume(Chunk chunk)
{
    if (!queued_eligible)
    {
        pending.push_back(std::move(chunk));
        return;
    }

    /// Passed through, not dropped: a zero-row chunk can still carry info an upstream transform
    /// attached (e.g. `RestoreChunkInfosTransform` after squashing).
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
            queued_eligible = false;
            pending.push_back(std::move(chunk));
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
            queued_eligible = false;
            pending.push_back(std::move(materialized));
        }
        else
        {
            held = std::move(materialized);
        }
        return;
    }

    /// A second non-empty block means the result is not a single block.
    LOG_DEBUG(logger, "INSERT ... SELECT will be executed synchronously (reason: the result is not a single block)");
    queued_eligible = false;
    pending.push_back(std::move(*held));
    held.reset();
    pending.push_back(std::move(chunk));
}

bool AsyncInsertQueueTransform::canGenerate()
{
    return !pending.empty();
}

AsyncInsertQueueTransform::GenerateResult AsyncInsertQueueTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(pending.front());
    pending.pop_front();
    res.is_done = pending.empty();
    return res;
}

AsyncInsertQueueTransform::GenerateResult AsyncInsertQueueTransform::getRemaining()
{
    if (held)
    {
        auto block = getInputPort().getHeader().cloneWithColumns(held->detachColumns());
        held.reset();

        auto async_query = query_ast->clone();
        auto & async_insert_query = async_query->as<ASTInsertQuery &>();
        /// `preprocessInsertQuery` rejects an empty format and a plain `INSERT ... SELECT` carries none.
        async_insert_query.format = "Native";
        async_insert_query.columns = make_intrusive<ASTExpressionList>();
        for (const auto & name : insert_column_names)
            async_insert_query.columns->children.push_back(make_intrusive<ASTIdentifier>(name));
        auto result = queue->pushQueryWithBlock(async_query, std::move(block), context);

        /// `wait_for_async_insert` is not honoured: returning early would hide a flush failure from a
        /// client already told the INSERT succeeded.
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

            if (isCancelled())
                return {};

            if (auto process_list_elem = context->getProcessListElement())
            {
                /// Called only for its throwing side effect (kill / `max_execution_time`); the block is
                /// already queued, so a `false` return under `'break'` is ignored.
                process_list_elem->checkTimeLimit();
            }
        }

        if (result.future.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout ({} ms) exceeded", wait_timeout_ms);

        /// The future is already ready, so the timeout argument below is unused; `report_read_progress`
        /// is false because the `SELECT` side already reported its own reads.
        waitForAsyncInsertAndReportProgress(
            result.future, /* future is already ready, unused for waiting */ 0,
            context->getProcessListElement(), context->getProgressCallback(),
            /* report_read_progress */ false);
    }

    return {};
}

}
