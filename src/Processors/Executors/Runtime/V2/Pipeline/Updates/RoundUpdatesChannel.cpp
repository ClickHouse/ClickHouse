#include <Processors/Executors/Runtime/V2/Pipeline/Updates/RoundUpdatesChannel.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Port.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>

namespace DB
{

namespace Runtime::V2
{

template <class PortT>
RoundUpdatesChannel<PortT>::RoundUpdatesChannel(const RoundUpdatesChannel & other)
{
    if (other.isConnected())
        Wyrd::abort("Cannot copy a port with a connected update channel");
}

template <class PortT>
void RoundUpdatesChannel<PortT>::connect(ExecutingProcessor & owner_, PortT & port_)
{
    if (isConnected())
        Wyrd::abort("Update channel is already connected");

    port = &port_;
    owner = &owner_;
}

template <class PortT>
void RoundUpdatesChannel<PortT>::disconnect()
{
    port = nullptr;
    owner = nullptr;
}

template <class PortT>
bool RoundUpdatesChannel<PortT>::isConnected() const
{
    return owner != nullptr;
}

template <class PortT>
ExecutingProcessor & RoundUpdatesChannel<PortT>::getOwner() const
{
    if (!isConnected())
        Wyrd::abort("Update channel is not connected: the port was connected outside a pipeline update");

    return *owner;
}

template <class PortT>
void RoundUpdatesChannel<PortT>::notifyChanges()
{
    if (!owner || in_round_updates)
        return;

    in_round_updates = true;
    owner->round_updates.push(*port);
}

template <class PortT>
void RoundUpdatesChannel<PortT>::markDrained()
{
    in_round_updates = false;
}

template class RoundUpdatesChannel<InputPort>;
template class RoundUpdatesChannel<OutputPort>;

}

}
