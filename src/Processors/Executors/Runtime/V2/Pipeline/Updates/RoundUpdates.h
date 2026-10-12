#pragma once

#include <Processors/IProcessor.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace Runtime::V2
{

class RoundUpdates
{
    template <class PortT>
    static void drain(VectorWithMemoryTracking<PortT *> & ports, VectorWithMemoryTracking<PortT *> & ports_out);

public:
    void push(InputPort & port);
    void push(OutputPort & port);
    void drain(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out);

private:
    IProcessor::UpdatedInputPorts inputs;
    IProcessor::UpdatedOutputPorts outputs;
};

}

}
