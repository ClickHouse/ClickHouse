#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/TCPServerParams.h>
#include <Poco/RefCountedObject.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadWithThreadStatus.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Core/ServerSettings.h>

#include <atomic>
#include <functional>


namespace DB
{
class Context;

class TCPServerConnectionFilter : public Poco::RefCountedObject
{
public:
    using Ptr = Poco::AutoPtr<TCPServerConnectionFilter>;

    explicit TCPServerConnectionFilter(std::function<bool()> filter_func_) : filter_func(std::move(filter_func_)) {}

    bool accept(const Poco::Net::StreamSocket &) const { return filter_func(); }

protected:
    ~TCPServerConnectionFilter() override = default;

private:
    std::function<bool()> filter_func;
};

/// Serves the connections of the TCP servers sharing it, at most `max_connections` at a time.
class TCPServerThreadPool : public ThreadPoolImpl<ThreadWithThreadStatus>
{
public:
    explicit TCPServerThreadPool(Int64 max_connections);
};

/// Accepts connections on `socket` and serves each one in a thread of `thread_pool`.
class TCPServer : public Poco::Runnable
{
public:
    explicit TCPServer(
        TCPServerConnectionFactory::Ptr factory,
        TCPServerThreadPool & thread_pool,
        Poco::Net::ServerSocket & socket,
        Poco::Net::TCPServerParams::Ptr params = new Poco::Net::TCPServerParams,
        const TCPServerConnectionFilter::Ptr & filter = nullptr);

    ~TCPServer() override;

    void start();

    /// Close the socket and ask existing connections to stop serving queries
    void stop();

    bool isOpen() const { return is_open; }

    UInt16 portNumber() const { return port_number; }

    const Poco::Net::ServerSocket& getSocket() { return socket; }

    /// Accepted connections that are being served or wait for a thread.
    size_t currentConnections() const { return current_connections; }

    /// Connections that are being served.
    size_t currentThreads() const { return current_threads; }

    size_t refusedConnections() const { return refused_connections; }

private:
    void run() override;

    void serveConnection(const Poco::Net::StreamSocket & connection_socket);

    TCPServerConnectionFactory::Ptr factory;
    TCPServerThreadPool & thread_pool;
    Poco::Net::ServerSocket socket;
    TCPServerConnectionFilter::Ptr filter;
    const bool no_delay;
    const size_t max_queued;
    const UInt16 port_number;
    Poco::Thread accept_thread;
    std::atomic<bool> accepting = false;
    std::atomic<bool> is_open = true;
    std::atomic<size_t> current_connections = 0;
    std::atomic<size_t> current_threads = 0;
    std::atomic<size_t> waiting_connections = 0;
    std::atomic<size_t> refused_connections = 0;
    LoggerPtr log;
};

}
