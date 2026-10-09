#include <Processors/Executors/Runtime/V2/Pipeline/Updates/RoundUpdates.h>
#include <Processors/Port.h>

#include <algorithm>

namespace DB
{

namespace Runtime::V2
{

template <class PortT>
void RoundUpdates::drainLifo(std::vector<PortT *> & ports, std::vector<PortT *> & ports_out)
{
    ports_out.clear();
    ports_out.swap(ports);
    std::reverse(ports_out.begin(), ports_out.end());

    for (auto * port : ports_out)
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

void RoundUpdates::drainLifo(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out)
{
    drainLifo(inputs, inputs_out);
    drainLifo(outputs, outputs_out);
}

}

}
