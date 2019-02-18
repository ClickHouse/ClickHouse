#include <Processors/Formats/IOutputFormat.h>


namespace DB
{

IOutputFormat::IOutputFormat(Block header, WriteBuffer & out)
    : IProcessor({std::move(header), std::move(header), std::move(header)}, {}), out(out)
{
}

IOutputFormat::Status IOutputFormat::prepare()
{
    if (has_input)
        return Status::Ready;

    for (auto kind : {Main, Totals, Extremes})
    {
        auto & input = inputs[kind];

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

    return Status::Finished;
}

void IOutputFormat::work()
{
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

}

