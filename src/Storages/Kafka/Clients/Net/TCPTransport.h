#pragma once

#include <Storages/Kafka/Clients/Net/Transport.h>

#include <Poco/Net/StreamSocket.h>

#include <memory>

namespace Kafka::Net
{

/// TCP transport over a non-blocking Poco::Net::StreamSocket.
///
/// Poco (and ClickHouse around it) provides socket lifecycle, connect, options
/// and address resolution (cached DB::DNSResolver). The waiting itself stays
/// a poll(2) over {socket fd, WakeupSource pipe} in deadline slices, so
/// Client::wakeup() interrupts blocked calls — the same raw-fd multiplexing
/// ClickHouse uses for cancellable reads (PacketReceiver,
/// RemoteQueryExecutorReadContext). The data path uses raw recv/send with an
/// explicit EINTR retry: Poco's non-blocking receiveBytes/sendBytes throw on
/// EINTR, which the query profiler signals would trigger spuriously.
class TCPTransport : public Transport
{
public:
    /// `wakeup` may be null (no interruption support, e.g. tests).
    explicit TCPTransport(std::shared_ptr<const WakeupSource> wakeup);

    void connect(const String & host, UInt16 port, const Deadline & deadline) override;
    size_t readSome(char * data, size_t size, const Deadline & deadline) override;
    void writeAll(std::string_view data, const Deadline & deadline) override;
    bool readable() override;
    void close() override;
    bool isOpen() const override { return socket.impl()->initialized(); }

protected:
    /// Wait until the socket is ready for `events` (POLLIN/POLLOUT), the
    /// deadline expires (throws Timeout), or a wakeup arrives (throws Wakeup).
    void waitReady(short events, const Deadline & deadline, const char * op);

    int sockFd() const { return static_cast<int>(socket.impl()->sockfd()); }

    std::shared_ptr<const WakeupSource> wakeup_source;
    /// TLSTransport swaps in a SecureStreamSocket after the TCP connect.
    Poco::Net::StreamSocket socket;
};

}
