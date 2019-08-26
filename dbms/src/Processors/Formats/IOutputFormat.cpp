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

        current_chunk = input.pull();
        current_block_kind = kind;
        has_input = true;
        return Status::Ready;
    }

    finished = true;

    if (!finalized)
        return Status::Ready;

    return Status::Finished;
}

void IOutputFormat::work()
{
    if (finished && !finalized)
    {
        finalize();
        finalized = true;
        return;
    }

    switch (current_block_kind)
    {
        case Main:
            consume(std::move(current_chunk));
            break;
        case Totals:
            consumeTotals(std::move(current_chunk));
            break;
        case Extremes:
            consumeExtremes(std::move(current_chunk));
            break;
    }

    has_input = false;
}

void IOutputFormat::flush()
{
    out.next();
}

}

