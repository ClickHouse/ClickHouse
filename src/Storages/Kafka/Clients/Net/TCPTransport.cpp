#include <Storages/Kafka/Clients/Net/TCPTransport.h>

#include <Common/DNSResolver.h>
#include <base/errnoToString.h>

#include <Poco/Exception.h>
#include <Poco/Net/SocketAddress.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>

/// macOS has no MSG_NOSIGNAL; Poco's initSocket sets SO_NOSIGPIPE there instead.
#if !defined(MSG_NOSIGNAL)
#define MSG_NOSIGNAL 0
#endif

namespace Kafka::Net
{

namespace
{

/// Upper bound of one poll slice: bounds wakeup-race latency and keeps
/// deadline checks responsive without burning CPU.
constexpr std::chrono::milliseconds poll_slice{100};

[[noreturn]] void throwErrno(const String & what)
{
    throw Exception(ErrorKind::Connection, "{}: {}", what, errnoToString());
}

}

TCPTransport::TCPTransport(std::shared_ptr<const WakeupSource> wakeup) : wakeup_source(std::move(wakeup))
{
    /// The default-constructed StreamSocket holds an uninitialized (closed)
    /// impl; the real socket is created by connect().
}

void TCPTransport::close()
{
    socket.close();
}

void TCPTransport::waitReady(short events, const Deadline & deadline, const char * op)
{
    const UInt64 start_generation = wakeup_source ? wakeup_source->getGeneration() : 0;
    for (;;)
    {
        if (wakeup_source && wakeup_source->getGeneration() != start_generation)
        {
            wakeup_source->drain();
            throw Exception(ErrorKind::Wakeup, "{} interrupted by wakeup()", op);
        }
        if (deadline.expired())
            throw Exception(ErrorKind::Timeout, "{} timed out", op);

        pollfd fds[2];
        fds[0] = {sockFd(), events, 0};
        nfds_t nfds = 1;
        if (wakeup_source)
        {
            fds[1] = {wakeup_source->getFd(), POLLIN, 0};
            nfds = 2;
        }

        const auto slice = deadline.remainingClamped(poll_slice);
        const int rc = ::poll(fds, nfds, static_cast<int>(slice.count()));
        if (rc < 0)
        {
            if (errno == EINTR)
                continue;
            throwErrno(fmt::format("{}: poll() failed", op));
        }
        if (rc == 0)
            continue; /// slice elapsed; loop re-checks deadline + wakeup
        if (wakeup_source && nfds == 2 && (fds[1].revents & POLLIN) != 0)
        {
            wakeup_source->drain();
            /// Generation check at loop top decides whether this wakeup is
            /// for us or a stale leftover.
            continue;
        }
        if ((fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 && (fds[0].revents & events) == 0)
            throw Exception(ErrorKind::Connection, "{}: connection reset by peer", op);
        if ((fds[0].revents & events) != 0)
            return;
    }
}

void TCPTransport::connect(const String & host, UInt16 port, const Deadline & deadline)
{
    close();

    /// NOTE: name resolution blocks without a deadline (getaddrinfo underneath).
    /// Acceptable for v1; DNSResolver at least caches the results, so repeated
    /// reconnects do not pay for it.
    std::vector<Poco::Net::SocketAddress> addresses;
    try
    {
        addresses = DB::DNSResolver::instance().resolveAddressList(host, port);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorKind::Connection, "Cannot resolve {}:{}: {}", host, port, e.displayText());
    }

    String last_error = "no addresses";
    for (const auto & address : addresses)
    {
        /// A fresh socket per attempt: a failed non-blocking connect leaves the
        /// old one unusable. Poco creates it non-blocking in connectNB() and has
        /// already set SO_NOSIGPIPE where that is the mechanism (macOS/FreeBSD).
        socket = Poco::Net::StreamSocket();
        try
        {
            socket.connectNB(address);
            socket.setNoDelay(true);
        }
        catch (const Poco::Exception & e)
        {
            last_error = e.displayText();
            socket.close();
            continue;
        }
        ::fcntl(sockFd(), F_SETFD, FD_CLOEXEC);

        try
        {
            waitReady(POLLOUT, deadline, "Connect");
        }
        catch (...)
        {
            close();
            throw;
        }
        const int so_error = socket.impl()->socketError();
        if (so_error == 0)
            return;
        last_error = fmt::format("connect(): {}", errnoToString(so_error));
        close();
    }
    throw Exception(ErrorKind::Connection, "Cannot connect to {}:{}: {}", host, port, last_error);
}

bool TCPTransport::readable()
{
    if (!isOpen())
        return false;
    pollfd pfd{sockFd(), POLLIN, 0};
    return ::poll(&pfd, 1, 0) > 0 && (pfd.revents & (POLLIN | POLLERR | POLLHUP)) != 0;
}

size_t TCPTransport::readSome(char * data, size_t size, const Deadline & deadline)
{
    /// Raw recv with an explicit EINTR retry instead of Poco's receiveBytes:
    /// on a non-blocking socket Poco throws on EINTR, and profiler signals
    /// would turn that into spurious connection failures.
    for (;;)
    {
        waitReady(POLLIN, deadline, "Read");
        const ssize_t n = ::recv(sockFd(), data, size, 0);
        if (n > 0)
            return static_cast<size_t>(n);
        if (n == 0)
            throw Exception(ErrorKind::Connection, "Connection closed by broker");
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            continue;
        throwErrno("Socket recv() failed");
    }
}

void TCPTransport::writeAll(std::string_view data, const Deadline & deadline)
{
    size_t written = 0;
    while (written < data.size())
    {
        waitReady(POLLOUT, deadline, "Write");
        const ssize_t n = ::send(sockFd(), data.data() + written, data.size() - written, MSG_NOSIGNAL);
        if (n > 0)
        {
            written += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
            continue;
        throwErrno("Socket send() failed");
    }
}

}
