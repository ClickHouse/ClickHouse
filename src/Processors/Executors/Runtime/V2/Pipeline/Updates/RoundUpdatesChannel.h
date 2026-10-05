#pragma once

namespace DB
{

class InputPort;
class OutputPort;

namespace Runtime::V2
{

struct ProcessorState;

template <class PortT>
class RoundUpdatesChannel
{
public:
    RoundUpdatesChannel() = default;
    RoundUpdatesChannel(const RoundUpdatesChannel & other);
    RoundUpdatesChannel & operator=(const RoundUpdatesChannel &) = delete;

    void connect(ProcessorState & owner_, PortT & port_);
    void disconnect();
    bool isConnected() const;
    ProcessorState & getOwner() const;

    void notifyChanges();
    void markDrained();

private:
    PortT * port = nullptr;
    ProcessorState * owner = nullptr;
    bool in_round_updates = false;
};

extern template class RoundUpdatesChannel<InputPort>;
extern template class RoundUpdatesChannel<OutputPort>;

}

}
