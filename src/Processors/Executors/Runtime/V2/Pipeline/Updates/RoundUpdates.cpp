#include <Processors/Executors/Runtime/V2/Pipeline/Updates/RoundUpdates.h>
#include <Processors/Port.h>

#include <algorithm>

namespace DB
{

namespace Runtime::V2
{

template <class PortT>
void RoundUpdates::drain(std::vector<PortT *> & ports, std::vector<PortT *> & ports_out_lifo)
{
    ports_out_lifo.clear();
    ports_out_lifo.swap(ports);
    std::reverse(ports_out_lifo.begin(), ports_out_lifo.end());

    for (auto * port : ports_out_lifo)
        port->getRoundUpdatesChannel().markDrained();
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

}
