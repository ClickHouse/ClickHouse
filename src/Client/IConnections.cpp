#include <Client/IConnections.h>
#include <Common/AsyncTaskExecutor.h>
#include <Poco/Net/SocketImpl.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SOCKET_TIMEOUT;
}

/// This wrapper struct allows us to use Poco's socket polling code with a raw fd.
/// The only difference from Poco::Net::SocketImpl is that we don't close the fd in the destructor.
struct PocoSocketWrapper : public Poco::Net::SocketImpl
{
    explicit PocoSocketWrapper(int fd)
    {
        reset(fd);
    }

    // Do not close fd.
    ~PocoSocketWrapper() override { reset(-1); }
};

void IConnections::DrainCallback::operator()(int fd, Poco::Timespan, AsyncEventTimeoutType, const std::string & fd_description, uint32_t events) const
{
    uint32_t poco_events = 0;
    if (events & AsyncTaskExecutor::Event::READ)
        poco_events |= Poco::Net::Socket::SELECT_READ;
    if (events & AsyncTaskExecutor::Event::WRITE)
        poco_events |= Poco::Net::Socket::SELECT_WRITE;
    if (events & AsyncTaskExecutor::Event::ERROR)
        poco_events |= Poco::Net::Socket::SELECT_ERROR;

    if (!PocoSocketWrapper(fd).poll(drain_timeout, poco_events))
    {
        throw Exception(ErrorCodes::SOCKET_TIMEOUT,
            "Read timeout ({} ms) while draining from {}",
            drain_timeout.totalMilliseconds(),
            fd_description);
    }
}

}
