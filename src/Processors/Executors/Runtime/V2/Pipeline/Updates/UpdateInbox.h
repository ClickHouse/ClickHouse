#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/Updates/UpdateInboxEntry.h>
#include <Processors/IProcessor.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace Runtime::V2
{

class UpdateInbox
{
    template <class PortT>
    static void drain(UpdateInboxEntry *& head, VectorWithMemoryTracking<PortT *> & ports_out);

public:
    UpdateInbox();

    void push(InputPort & port);
    void push(OutputPort & port);
    void drain(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out);

private:
    UpdateInboxEntry * inputs;
    UpdateInboxEntry * outputs;
};

}

}
