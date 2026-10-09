#include <Processors/Executors/Runtime/V2/Pipeline/Updates/UpdateInbox.h>
#include <Processors/Port.h>

#include <utility>

namespace DB
{

namespace Runtime::V2
{

static UpdateInboxEntry bottom;

UpdateInbox::UpdateInbox()
    : inputs(&bottom)
    , outputs(&bottom)
{
}

template <class PortT>
void UpdateInbox::drainLifo(UpdateInboxEntry *& head, std::vector<PortT *> & ports_out)
{
    ports_out.clear();

    for (UpdateInboxEntry * entry = std::exchange(head, &bottom); entry != &bottom; entry = std::exchange(entry->next, nullptr))
        ports_out.push_back(entry->as<PortT>());
}

void UpdateInbox::push(InputPort & port)
{
    if (port.next)
        return;

    port.next = inputs;
    inputs = &port;
}

void UpdateInbox::push(OutputPort & port)
{
    if (port.next)
        return;

    port.next = outputs;
    outputs = &port;
}

void UpdateInbox::drainLifo(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out)
{
    drainLifo(inputs, inputs_out);
    drainLifo(outputs, outputs_out);
}

}

}
