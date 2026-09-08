#pragma once

namespace DB
{

class InputPort;
class OutputPort;
struct ProcessorState;

template <class PortT>
class UpdateChannel
{
public:
    UpdateChannel() = default;
    UpdateChannel(const UpdateChannel & other);
    UpdateChannel & operator=(const UpdateChannel &) = delete;

    void connect(ProcessorState & owner_, PortT & port_);
    void disconnect();
    bool isConnected() const;
    ProcessorState & getOwner() const;

    void notifyChanges();
    void resetNotified();

private:
    PortT * port = nullptr;
    ProcessorState * owner = nullptr;
    bool already_notified = false;
};

extern template class UpdateChannel<InputPort>;
extern template class UpdateChannel<OutputPort>;

}
