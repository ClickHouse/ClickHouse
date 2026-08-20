#include <Processors/Port.h>
#include <Processors/Transforms/InputSelectorTransform.h>

namespace DB
{

static InputPorts createInputPorts(const Block & header)
{
    InputPorts ports;
    ports.emplace_back(Block{}); /// signal
    ports.emplace_back(header); /// true branch
    ports.emplace_back(header); /// false branch
    return ports;
}

static OutputPorts createOutputPorts(const Block & header)
{
    OutputPorts ports;
    ports.emplace_back(header);
    return ports;
}

InputSelectorTransform::InputSelectorTransform(const Block & header)
    : IProcessor(createInputPorts(header), createOutputPorts(header))
    , signal_port(inputs.front())
    , true_port(*std::next(inputs.begin()))
    , false_port(inputs.back())
{
}

IProcessor::Status InputSelectorTransform::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        signal_port.close();
        true_port.close();
        false_port.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (state == State::WaitingForSignal)
    {
        signal_port.setNeeded();

        if (signal_port.hasData())
        {
            signal_port.pull();
            /// Signal received — select the true branch.
            selected_port = &true_port;
            false_port.close();
            signal_port.close();
            state = State::Forwarding;
        }
        else if (signal_port.isFinished())
        {
            /// No signal — select the false branch.
            selected_port = &false_port;
            true_port.close();
            state = State::Forwarding;
        }
        else
        {
            return Status::NeedData;
        }
    }

    /// Forwarding state.

    if (selected_port->isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    selected_port->setNeeded();
    if (!selected_port->hasData())
        return Status::NeedData;

    output.push(selected_port->pull());
    return Status::PortFull;
}

}
