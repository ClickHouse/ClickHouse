#include <Processors/Transforms/AsyncInsertQueueTransform.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>

namespace DB
{

namespace
{

/// Cancellation checkpoint granularity for the flush wait. `wait_for` returns as soon as the future
/// is ready, so this adds no latency; it only bounds how long the query is uninterruptible. Same
/// pattern as the stage wait in `DistributedPlanExecutor`.
constexpr auto flush_wait_cancellation_check_interval = std::chrono::milliseconds(100);

/// Size the block would take once queued, without expanding it, mirroring what
/// `removeSpecialRepresentations` expands: a replicated column's nested column, tuple elements
/// recursively, and const or sparse columns. Materializing to find the size out would allocate the
/// expansion even for a block that is about to be rejected.
size_t estimateMaterializedBytes(const ColumnPtr & column, size_t rows)
{
    if (const auto * replicated = typeid_cast<const ColumnReplicated *>(column.get()))
    {
        const auto & nested = replicated->getNestedColumn();
        const size_t stored_rows = std::max<size_t>(1, nested->size());
        return estimateMaterializedBytes(nested, nested->size()) / stored_rows * rows;
    }

    if (const auto * tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        size_t total = 0;
        for (const auto & element : tuple->getColumns())
            total += estimateMaterializedBytes(element, rows);
        return total;
    }

    /// `byteSize` counts only the values actually stored. `byteSizeAt(0)` is the per-row cost the
    /// expansion adds on top: for a sparse column row 0 is normally its default, which is what each
    /// expanded gap costs.
    if (column->isConst() || column->isSparse())
        return column->byteSize() + column->byteSizeAt(0) * rows;

    return column->byteSize();
}

size_t estimateMaterializedBytes(const Columns & columns, size_t rows)
{
    size_t total = 0;
    for (const auto & column : columns)
        total += estimateMaterializedBytes(column, rows);
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
{
}

void AsyncInsertQueueTransform::onConsume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    if (!queued_eligible)
    {
        pending.push_back(std::move(chunk));
        return;
    }

    if (!held)
    {
        if (estimateMaterializedBytes(chunk.getColumns(), chunk.getNumRows()) > max_data_size)
        {
            queued_eligible = false;
            pending.push_back(std::move(chunk));
            return;
        }

        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        materializeBlockInplace(block);
        Chunk materialized(block.getColumns(), block.rows());

        /// The estimate is per value, so a column of varying-width values can still overshoot.
        if (block.bytes() > max_data_size)
        {
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
        /// The pushed block is Preprocessed (Native-encoded). `preprocessInsertQuery` rejects an
        /// empty format, and a plain `INSERT ... SELECT` carries none, so set `Native` explicitly.
        async_insert_query.format = "Native";
        async_insert_query.columns = make_intrusive<ASTExpressionList>();
        for (const auto & name : insert_column_names)
            async_insert_query.columns->children.push_back(make_intrusive<ASTIdentifier>(name));
        auto result = queue->pushQueryWithBlock(async_query, std::move(block), context);

        /// The wait is unconditional, `wait_for_async_insert` is deliberately not honoured on this
        /// route: returning early would hide a flush failure from a client whose INSERT ... SELECT
        /// already reported success. Poll instead of blocking for the whole timeout, so `KILL QUERY`
        /// and `max_execution_time` still end the query. The entry is queued by now and gets flushed
        /// either way, so cancellation stops the waiting, not the write.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_timeout_ms);
        while (result.future.wait_for(flush_wait_cancellation_check_interval) == std::future_status::timeout)
        {
            if (isCancelled())
                return {};

            if (auto process_list_elem = context->getProcessListElement())
            {
                process_list_elem->checkTimeLimit();
                process_list_elem->throwIfKilled();
            }

            if (std::chrono::steady_clock::now() >= deadline)
                break;
        }

        const auto now = std::chrono::steady_clock::now();
        const UInt64 remaining_ms
            = now >= deadline ? 0 : std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();

        /// `report_read_progress=false`: reads were already counted by `CountingTransform`.
        waitForAsyncInsertAndReportProgress(
            result.future, remaining_ms,
            context->getProcessListElement(), context->getProgressCallback(),
            /* report_read_progress */ false);
    }

    return {};
}

}
