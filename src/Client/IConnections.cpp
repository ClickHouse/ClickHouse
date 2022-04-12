#include <Client/IConnections.h>
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

void IConnections::DrainCallback::operator()(int fd, Poco::Timespan, const std::string & fd_description) const
{
    if (!PocoSocketWrapper(fd).poll(drain_timeout, Poco::Net::Socket::SELECT_READ))
    {
        throw Exception(ErrorCodes::SOCKET_TIMEOUT,
            "Read timeout ({} ms) while draining from {}",
            drain_timeout.totalMilliseconds(),
            fd_description);
    }
}

}
