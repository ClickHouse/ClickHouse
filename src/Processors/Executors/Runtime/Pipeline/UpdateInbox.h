#pragma once

#include <Processors/Executors/Runtime/Pipeline/UpdateInboxEntry.h>
#include <Processors/IProcessor.h>

#include <atomic>

namespace DB
{

class UpdateInbox
{
    class Stack
    {
    public:
        void push(UpdateInboxEntry & entry);
        UpdateInboxEntry * drain();

        bool isEmpty() const;

    private:
        std::atomic<UpdateInboxEntry *> head{nullptr};
    };

    template <class PortT>
    static void drain(Stack & stack, std::vector<PortT *> & ports_out);

public:
    void push(InputPort & port);
    void push(OutputPort & port);

    void drain(IProcessor::UpdatedInputPorts & inputs_out, IProcessor::UpdatedOutputPorts & outputs_out);

private:
    Stack inputs;
    Stack outputs;
};

}
