#include <Processors/Formats/IOutputFormat.h>
#include <IO/WriteBuffer.h>


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
    if (!prefix_written)
    {
        doWritePrefix();
        prefix_written = true;
    }

    if (finished && !finalized)
    {
        if (rows_before_limit_counter && rows_before_limit_counter->hasAppliedLimit())
            setRowsBeforeLimit(rows_before_limit_counter->get());

        finalize();
        finalized = true;
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
            if (auto totals = prepareTotals(std::move(current_chunk)))
                consumeTotals(std::move(totals));
            break;
        case Extremes:
            consumeExtremes(std::move(current_chunk));
            break;
    }

    if (auto_flush)
        flush();

    has_input = false;
}

void IOutputFormat::flush()
{
    out.next();
}

void IOutputFormat::write(const Block & block)
{
    consume(Chunk(block.getColumns(), block.rows()));

    if (auto_flush)
        flush();
}

}
