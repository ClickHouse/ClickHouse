#pragma once

#include <Poco/Net/StreamSocket.h>

class RemoteFSConnection
{
    friend class MultiplexedConnections;

public:
    RemoteFSConnection(const String & host_, UInt16 port_,
        const String & disk_name_);

    ~RemoteFSConnection() override;

    /// Set throttler of network traffic. One throttler could be used for multiple connections to limit total traffic.
    void setThrottler(const ThrottlerPtr & throttler_) override
    {
        throttler = throttler_;
    }

    /// For log and exception messages.
    const String & getDescription() const override;
    const String & getHost() const;
    UInt16 getPort() const;

    // TODO send requests

    // TODO check if needed
    // void forceConnected(const ConnectionTimeouts & timeouts) override; 

    bool isConnected() const override { return connected; }

    bool checkConnected(const ConnectionTimeouts & timeouts) override { return connected && ping(timeouts); }

    void disconnect() override;

    size_t outBytesCount() const { return out ? out->count() : 0; }
    size_t inBytesCount() const { return in ? in->count() : 0; }

    Poco::Net::Socket * getSocket() { return socket.get(); }

    /// Each time read from socket blocks and async_callback is set, it will be called. You can poll socket inside it.
    void setAsyncCallback(AsyncCallback async_callback_)
    {
        async_callback = std::move(async_callback_);
        if (in)
            in->setAsyncCallback(std::move(async_callback));
    }
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
        explicit LoggerWrapper(Connection & parent_)
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
        Connection & parent;
    };

    LoggerWrapper log_wrapper;

    void connect(const ConnectionTimeouts & timeouts);
    void sendHello();
    void receiveHello();

    bool ping(const ConnectionTimeouts & timeouts);

    Block receiveData();
    Block receiveLogData();

    std::unique_ptr<Exception> receiveException() const;

    [[noreturn]] void throwUnexpectedPacket(UInt64 packet_type, const char * expected) const;
};