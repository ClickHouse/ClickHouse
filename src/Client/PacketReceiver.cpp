#include <Client/PacketReceiver.h>

#if defined(OS_LINUX)

namespace DB
{

PacketReceiver::PacketReceiver(Connection * connection_) : AsyncTaskExecutor(std::make_unique<Task>(*this)), connection(connection_)
{
    epoll.add(timeout_descriptor.getDescriptor());
    socket_fd = connection->getSocket()->impl()->sockfd();
    epoll.add(socket_fd);
}

bool PacketReceiver::checkBeforeTaskResume()
{
    /// If there is no pending data, check timeout.
    return connection->hasReadPendingData() || checkTimeout();
}

void PacketReceiver::processAsyncEvent(int fd [[maybe_unused]], Poco::Timespan socket_timeout, AsyncEventTimeoutType, const std::string &, uint32_t)
{
    assert(fd == socket_fd);
    timeout_descriptor.setRelative(socket_timeout);
    timeout = socket_timeout;
    is_read_in_process = true;
}

void PacketReceiver::clearAsyncEvent()
{
    is_read_in_process = false;
    timeout_descriptor.reset();
}

bool PacketReceiver::checkTimeout()
{
    bool is_socket_ready = false;

    epoll_event events[2];
    events[0].data.fd = events[1].data.fd = -1;
    size_t ready_count = epoll.getManyReady(2, events, true);

    for (size_t i = 0; i != ready_count; ++i)
    {
        if (events[i].data.fd == socket_fd)
            is_socket_ready = true;
        if (events[i].data.fd == timeout_descriptor.getDescriptor())
            is_timeout_expired = true;
    }

    if (is_timeout_expired && !is_socket_ready)
    {
        timeout_descriptor.reset();
        return false;
    }

    return true;
}

void PacketReceiver::Task::run(AsyncCallback async_callback, SuspendCallback suspend_callback)
{
    while (true)
    {
        {
            AsyncCallbackSetter<Connection> async_setter(receiver.connection, async_callback);
            receiver.packet = receiver.connection->receivePacket();
        }
        suspend_callback();
    }
}

}

#endif
