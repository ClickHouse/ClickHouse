#include <Processors/Executors/Runtime/Pipeline/RoundUpdates.h>
#include <Processors/Port.h>

namespace DB
{

template <class PortT>
void RoundUpdates::drain(std::vector<PortT *> & ports, std::vector<PortT *> & ports_out)
{
    ports_out.clear();
    ports_out.swap(ports);

    for (auto * port : ports_out)
        port->getUpdateChannel().resetNotified();
}

void RoundUpdates::push(InputPort & port)
{
    inputs.push_back(&port);
}

void RoundUpdates::push(OutputPort & port)
{
    outputs.push_back(&port);
}

void RoundUpdates::drain(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out)
{
    drain(inputs, inputs_out);
    drain(outputs, outputs_out);
}

}
