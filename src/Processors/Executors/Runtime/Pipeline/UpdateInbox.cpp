#include <Processors/Executors/Runtime/Pipeline/UpdateInbox.h>
#include <Processors/Port.h>

namespace DB
{

void UpdateInbox::Stack::push(UpdateInboxEntry & entry)
{
    if (entry.already_in_inbox.exchange(true))
        return;

    entry.next = head.load();
    while (!head.compare_exchange_weak(entry.next, &entry))
    {
    }
}

UpdateInboxEntry * UpdateInbox::Stack::drain()
{
    UpdateInboxEntry * reversed = nullptr;
    UpdateInboxEntry * entry = head.exchange(nullptr);

    while (entry)
    {
        UpdateInboxEntry * next = entry->next;
        entry->next = reversed;
        reversed = entry;
        entry = next;
    }

    return reversed;
}

bool UpdateInbox::Stack::isEmpty() const
{
    return head.load() == nullptr;
}

template <class PortT>
void UpdateInbox::drain(Stack & stack, std::vector<PortT *> & ports_out)
{
    ports_out.clear();

    UpdateInboxEntry * entry = stack.drain();
    while (entry)
    {
        UpdateInboxEntry * next = entry->next;
        entry->already_in_inbox.store(false);
        ports_out.push_back(entry->as<PortT>());
        entry = next;
    }
}

void UpdateInbox::push(InputPort & port)
{
    inputs.push(port);
}

void UpdateInbox::push(OutputPort & port)
{
    outputs.push(port);
}

void UpdateInbox::drain(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out)
{
    drain(inputs, inputs_out);
    drain(outputs, outputs_out);
}

}
