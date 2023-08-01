#include <Processors/Formats/IOutputFormat.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

IOutputFormat::IOutputFormat(const Block & header_, WriteBuffer & out_)
    : IProcessor({header_, header_, header_, header_}, {}), out(out_)
{
}

IOutputFormat::Status IOutputFormat::prepare()
{
    if (has_input)
        return Status::Ready;

    auto status = prepareMainAndPartialResult();
    if (status != Status::Finished)
        return status;

    status = prepareTotalsAndExtremes();
    if (status != Status::Finished)
        return status;

    finished = true;

    if (!finalized)
        return Status::Ready;

    return Status::Finished;
}

IOutputFormat::Status IOutputFormat::prepareMainAndPartialResult()
{
    bool need_data = false;
    for (auto kind : {Main, PartialResult})
    {
        auto & input = getPort(kind);

        if (input.isFinished())
            continue;

        if (kind == PartialResult && was_main_input)
        {
            input.close();
            continue;
        }

        input.setNeeded();
        need_data = true;

        if (!input.hasData())
            continue;

        setCurrentChunk(input, kind);
        return Status::Ready;
    }

    if (need_data)
        return Status::NeedData;

    return Status::Finished;
}

IOutputFormat::Status IOutputFormat::prepareTotalsAndExtremes()
{
    for (auto kind : {Totals, Extremes})
    {
        auto & input = getPort(kind);

        if (!input.isConnected() || input.isFinished())
            continue;

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        setCurrentChunk(input, kind);
        return Status::Ready;
    }

    return Status::Finished;
}

void IOutputFormat::setCurrentChunk(InputPort & input, PortKind kind)
{
    current_chunk = input.pull(true);
    current_block_kind = kind;
    has_input = true;
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
    writePrefixIfNeeded();

    if (finished && !finalized)
    {
        if (rows_before_limit_counter && rows_before_limit_counter->hasAppliedLimit())
            setRowsBeforeLimit(rows_before_limit_counter->get());

        finalize();
        if (auto_flush)
            flush();
        return;
    }

    switch (current_block_kind)
    {
        case Main:
            result_rows += current_chunk.getNumRows();
            result_bytes += current_chunk.allocatedBytes();
            if (is_partial_result_protocol_active && !was_main_input && current_chunk.hasRows())
            {
                consume(Chunk(current_chunk.cloneEmptyColumns(), 0));
                was_main_input = true;
            }
            consume(std::move(current_chunk));
            break;
        case PartialResult:
            consumePartialResult(std::move(current_chunk));
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
        flush();

    has_input = false;
}

void IOutputFormat::flush()
{
    out.next();
}

void IOutputFormat::write(const Block & block)
{
    writePrefixIfNeeded();
    consume(Chunk(block.getColumns(), block.rows()));

    if (auto_flush)
        flush();
}

void IOutputFormat::writePartialResult(const Block & block)
{
    writePrefixIfNeeded();
    consumePartialResult(Chunk(block.getColumns(), block.rows()));

    if (auto_flush)
        flush();
}

void IOutputFormat::finalize()
{
    if (finalized)
        return;
    writePrefixIfNeeded();
    writeSuffixIfNeeded();
    finalizeImpl();
    finalizeBuffers();
    finalized = true;
}

void IOutputFormat::clearLastLines(size_t lines_number)
{
    /// http://en.wikipedia.org/wiki/ANSI_escape_code
    #define MOVE_TO_PREV_LINE "\033[A"
    #define CLEAR_TO_END_OF_LINE "\033[K"

    static const char * clear_prev_line = MOVE_TO_PREV_LINE \
                                          CLEAR_TO_END_OF_LINE;

    /// Move cursor to the beginning of line
    writeCString("\r", out);

    for (size_t line = 0; line < lines_number; ++line)
    {
        writeCString(clear_prev_line, out);
    }
}

}
