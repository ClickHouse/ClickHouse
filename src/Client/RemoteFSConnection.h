#pragma once

#include <Common/logger_useful.h>

#include <Poco/Net/StreamSocket.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

namespace DB
{

class RemoteFSConnection
{
    friend class MultiplexedConnections;

public:
    RemoteFSConnection(const String & host_, UInt16 port_,
        const String & disk_name_);

    ~RemoteFSConnection();

    /// For log and exception messages.
    const String & getDescription() const;
    const String & getHost() const;
    UInt16 getPort() const;

    // TODO send requests

    // TODO check if needed
    // void forceConnected(const ConnectionTimeouts & timeouts) override; 

    bool isConnected() const { return connected; }

    bool checkConnected(const ConnectionTimeouts & timeouts) { return connected && ping(timeouts); }

    void disconnect();

    size_t outBytesCount() const { return out ? out->count() : 0; }
    size_t inBytesCount() const { return in ? in->count() : 0; }

    Poco::Net::Socket * getSocket() { return socket.get(); }

private:
    String host;
    UInt16 port;
    String disk_name;

    /// Address is resolved during the first connection (or the following reconnects)
    /// Use it only for logging purposes
    std::optional<Poco::Net::SocketAddress> current_resolved_address;

    /// For messages in log and in exceptions.
    String description;
    void setDescription();

    /// Returns resolved address if it was resolved.
    std::optional<Poco::Net::SocketAddress> getResolvedAddress() const;

    bool connected = false;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    /// Logger is created lazily, for avoid to run DNS request in constructor.
    class LoggerWrapper
    {
    public:
        explicit LoggerWrapper(RemoteFSConnection & parent_)
            : log(nullptr), parent(parent_)
        {
        }

        Poco::Logger * get()
        {
            if (!log)
                log = &Poco::Logger::get("Connection (" + parent.getDescription() + ")");

            return log;
        }

    private:
        std::atomic<Poco::Logger *> log;
        RemoteFSConnection & parent;
    };

    LoggerWrapper log_wrapper;

    void connect(const ConnectionTimeouts & timeouts);
    void sendHello();
    void receiveHello();

    bool ping(const ConnectionTimeouts & timeouts);

    std::unique_ptr<Exception> receiveException() const;

    [[noreturn]] void throwUnexpectedPacket(UInt64 packet_type, const char * expected) const;
};

}
