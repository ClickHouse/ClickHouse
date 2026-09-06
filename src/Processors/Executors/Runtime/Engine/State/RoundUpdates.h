#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class RoundUpdates
{
    template <class PortT>
    static void drain(std::vector<PortT *> & ports, std::vector<PortT *> & ports_out);

public:
    void push(InputPort & port);
    void push(OutputPort & port);

    void drain(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out);

private:
    IProcessor::UpdatedInputPorts inputs;
    IProcessor::UpdatedOutputPorts outputs;
};

}
