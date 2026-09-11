#include <Processors/Executors/Runtime/Pipeline/UpdateChannel.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <class PortT>
UpdateChannel<PortT>::UpdateChannel(const UpdateChannel & other)
{
    if (other.isConnected())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot copy a port with a connected update channel");
}

template <class PortT>
void UpdateChannel<PortT>::connect(ProcessorState & owner_, PortT & port_)
{
    if (isConnected())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Update channel is already connected");

    port = &port_;
    owner = &owner_;
}

template <class PortT>
void UpdateChannel<PortT>::disconnect()
{
    port = nullptr;
    owner = nullptr;
}

template <class PortT>
bool UpdateChannel<PortT>::isConnected() const
{
    return owner != nullptr;
}

template <class PortT>
ProcessorState & UpdateChannel<PortT>::getOwner() const
{
    if (!isConnected())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Update channel is not connected");

    return *owner;
}

template <class PortT>
void UpdateChannel<PortT>::notifyChanges()
{
    if (!owner || already_notified)
        return;

    already_notified = true;
    owner->round_updates.push(*port);
}

template <class PortT>
void UpdateChannel<PortT>::resetNotified()
{
    already_notified = false;
}

template class UpdateChannel<InputPort>;
template class UpdateChannel<OutputPort>;

}
