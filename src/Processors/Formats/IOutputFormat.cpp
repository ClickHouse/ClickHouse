#include <Columns/IColumn.h>
#include <Processors/Formats/IOutputFormat.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>


namespace DB
{

IOutputFormat::IOutputFormat(const Block & header_, WriteBuffer & out_)
    : IProcessor({header_, header_, header_}, {}), out(out_)
{
}

IOutputFormat::Status IOutputFormat::prepare()
{
    if (has_input)
        return Status::Ready;

    for (auto kind : {Main, Totals, Extremes})
    {
        auto & input = getPort(kind);

        if (kind != Main && !input.isConnected())
            continue;

        if (input.isFinished())
            continue;

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        current_chunk = input.pull(true);
        current_block_kind = kind;
        has_input = true;
        return Status::Ready;
    }

    finished = true;

    if (!finalized)
        return Status::Ready;

    return Status::Finished;
}

static Chunk prepareTotals(Chunk chunk)
{
    if (!chunk.hasRows())
        return {};

    if (chunk.getNumRows() > 1)
    {
        /// This may happen if something like ARRAY JOIN was executed on totals.
        /// Skip rows except the first one.
        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
    }

    return chunk;
}

void IOutputFormat::work()
{
    std::lock_guard lock(writing_mutex);

    if (has_progress_update_to_write)
    {
        writeProgress(statistics.progress);
        has_progress_update_to_write = false;
    }

    writePrefixIfNeeded();

    if (finished && !finalized)
    {
        if (rows_before_limit_counter && rows_before_limit_counter->hasAppliedStep())
            setRowsBeforeLimit(rows_before_limit_counter->get());
        if (rows_before_aggregation_counter && rows_before_aggregation_counter->hasAppliedStep())
            setRowsBeforeAggregation(rows_before_aggregation_counter->get());
        finalizeUnlocked();
        if (auto_flush)
            flushImpl();
        return;
    }

    switch (current_block_kind)
    {
        case Main:
            result_rows += current_chunk.getNumRows();
            result_bytes += current_chunk.allocatedBytes();
            consume(std::move(current_chunk));
            break;
        case Totals:
            writeSuffixIfNeeded();
            if (auto totals = prepareTotals(std::move(current_chunk)))
            {
                consumeTotals(std::move(totals));
                are_totals_written = true;
            }
            break;
        case Extremes:
            writeSuffixIfNeeded();
            consumeExtremes(std::move(current_chunk));
            break;
    }

    if (auto_flush)
        flushImpl();

    has_input = false;
}

void IOutputFormat::flushImpl()
{
    out.next();

    /// If output is a compressed buffer, we will flush the compressed chunk as well.
    if (auto * out_with_nested = dynamic_cast<WriteBufferWithOwnMemoryDecorator *>(&out))
        out_with_nested->getNestedBuffer()->next();
}

void IOutputFormat::flush()
{
    std::lock_guard lock(writing_mutex);
    flushImpl();
}

void IOutputFormat::write(const Block & block)
{
    std::lock_guard lock(writing_mutex);

    if (has_progress_update_to_write)
    {
        writeProgress(statistics.progress);
        has_progress_update_to_write = false;
    }

    writePrefixIfNeeded();
    consume(Chunk(block.getColumns(), block.rows()));

    if (auto_flush)
        flushImpl();
}

void IOutputFormat::finalizeUnlocked()
{
    if (finalized)
        return;
    writePrefixIfNeeded();

    if (has_progress_update_to_write)
    {
        writeProgress(statistics.progress);
        has_progress_update_to_write = false;
    }

    writeSuffixIfNeeded();
    finalizeImpl();
    finalizeBuffers();
    finalized = true;
}

void IOutputFormat::finalize()
{
    std::lock_guard lock(writing_mutex);
    finalizeUnlocked();
}

void IOutputFormat::onProgress(const Progress & progress)
{
    statistics.progress.incrementPiecewiseAtomically(progress);
    UInt64 elapsed_ns = statistics.watch.elapsedNanoseconds();
    statistics.progress.elapsed_ns = elapsed_ns;
    if (writesProgressConcurrently())
    {
        has_progress_update_to_write = true;

        /// Do not write progress too frequently.
        if (elapsed_ns >= prev_progress_write_ns + 1000 * progress_write_frequency_us)
        {
            std::unique_lock lock(writing_mutex, std::try_to_lock);
            if (lock)
            {
                writeProgress(statistics.progress);
                flushImpl();
                prev_progress_write_ns = elapsed_ns;
                has_progress_update_to_write = false;
            }
        }
    }
}

void IOutputFormat::setProgress(Progress progress)
{
    statistics.progress = std::move(progress);
}

}
