#include <Processors/BarrierProcessor.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BarrierProcessor::BarrierProcessor() : IProcessor{InputPorts{}, OutputPorts{}}
{
}

InputPort & BarrierProcessor::addInputPort()
{
    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add input port to the started BarrierProcessor");
    return inputs.emplace_back(Block{}, this);
}

OutputPort & BarrierProcessor::addOutputPort()
{
    if (prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add output port to the started BarrierProcessor");
    return outputs.emplace_back(Block{}, this);
}

IProcessor::Status BarrierProcessor::prepare()
{
    if (!prepared)
    {
        for (auto && input : inputs)
            input.setNeeded();
        next_non_finished = inputs.begin();
        prepared = true;
    }

    while (next_non_finished != inputs.end() && next_non_finished->isFinished())
        ++next_non_finished;

    if (next_non_finished == inputs.end())
    {
        for (OutputPort& output : outputs)
            output.finish();
        return Status::Finished;
    }

    return Status::NeedData;
}

}
