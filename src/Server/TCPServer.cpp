#include <Server/TCPServer.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

#include <Poco/Exception.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Timespan.h>


namespace CurrentMetrics
{
    extern const Metric TCPServerThreads;
    extern const Metric TCPServerThreadsActive;
    extern const Metric TCPServerThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{

size_t checkMaxConnections(Int64 max_connections)
{
    if (max_connections < 1)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Setting `max_connections` must be at least 1, got {}", max_connections);
    return static_cast<size_t>(max_connections);
}

}

TCPServerThreadPool::TCPServerThreadPool(Int64 max_connections)
    : ThreadPoolImpl(
        CurrentMetrics::TCPServerThreads,
        CurrentMetrics::TCPServerThreadsActive,
        CurrentMetrics::TCPServerThreadsScheduled,
        checkMaxConnections(max_connections),
        /* max_free_threads = */ std::min<size_t>(checkMaxConnections(max_connections), 3),
        /* queue_size = */ 0,
        /* shutdown_on_exception = */ false)
{
}

TCPServer::TCPServer(
    TCPServerConnectionFactory::Ptr factory_,
    TCPServerThreadPool & thread_pool_,
    Poco::Net::ServerSocket & socket_,
    Poco::Net::TCPServerParams::Ptr params,
    const TCPServerConnectionFilter::Ptr & filter_)
    : factory(factory_)
    , thread_pool(thread_pool_)
    , socket(socket_)
    , filter(filter_)
    , no_delay(!params || params->getNoDelay())
    , max_queued(params ? static_cast<size_t>(params->getMaxQueued()) : 64)
    , port_number(socket.address().port())
    , accept_thread("TCPServer: " + socket.address().toString())
    , log(getLogger("TCPServer"))
{
}

TCPServer::~TCPServer()
{
    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void TCPServer::start()
{
    chassert(!accepting);
    accepting = true;
    accept_thread.start(*this);
}

void TCPServer::stop()
{
    if (!is_open)
        return;

    {
        /// The accept thread uses this object: it is joined even if `shutdownReceive` throws, and a failed join terminates.
        SCOPE_EXIT({ if (accepting.exchange(false)) accept_thread.join(); });

        // FIXME: On Darwin and illumos, shutdown(SHUT_RD) on a listening socket
        // returns ENOTCONN, so accept() is not explicitly awakened.
#if !defined(OS_DARWIN) && !defined(OS_SUNOS)
        // Shutdown the listen socket before stopping tcp server to avoid 2.5second delay
        socket.shutdownReceive();
#endif
    }

    // This notifies already established connections that they should stop serving
    // queries and close their socket as soon as they can.
    is_open = false;
    // Stopping the accept thread leaves the socket open.
    // To be able to hand over control of the listening port to a new server, and
    // to get fast connection refusal instead of timeouts, we also need to close
    // the listening socket.
    socket.close();
}

void TCPServer::run()
{
    auto fault_injection_blocker = CannotAllocateThreadFaultInjector::blockFaultInjections();

    while (accepting)
    {
        Poco::Timespan timeout(250000);
        try
        {
            if (socket.poll(timeout, Poco::Net::Socket::SELECT_READ))
            {
                try
                {
                    Poco::Net::StreamSocket connection_socket = socket.acceptConnection();

                    if (filter && !filter->accept(connection_socket))
                    {
                        LOG_WARNING(log, "Filtered out connection from {}", connection_socket.peerAddress().toString());
                        continue;
                    }

#if defined(POCO_OS_FAMILY_UNIX)
                    if (connection_socket.address().family() != Poco::Net::AddressFamily::UNIX_LOCAL)
#endif
                        connection_socket.setNoDelay(no_delay);

                    if (waiting_connections >= max_queued)
                    {
                        ++refused_connections;
                        LOG_WARNING(log, "Refusing connection from {}, reached max queue size {}",
                            connection_socket.peerAddress().toString(), max_queued);
                        continue;
                    }

                    /// Created before the counters are incremented, because creating it may throw.
                    TCPServerThreadPool::Job job = [this, connection_socket] { serveConnection(connection_socket); };
                    ++current_connections;
                    ++waiting_connections;
                    if (!thread_pool.trySchedule(std::move(job)))
                    {
                        --waiting_connections;
                        --current_connections;
                        ++refused_connections;
                        LOG_WARNING(log, "Refusing connection from {}: cannot start a thread for it",
                            connection_socket.peerAddress().toString());
                    }
                }
                catch (const Poco::InvalidArgumentException & e)
                {
                    /// Termination request
                    LOG_INFO(log, "Shutting down TCPServer: {}", e.displayText());
                }
                catch (...)
                {
                    tryLogCurrentException(log);
                }
            }
        }
        catch (const Poco::Exception &)
        {
            tryLogCurrentException(log);
            // possibly a resource issue since poll() failed;
            // give some time to recover before trying again
            Poco::Thread::sleep(50);
        }
    }
}

void TCPServer::serveConnection(const Poco::Net::StreamSocket & connection_socket)
{
    --waiting_connections;
    /// A connection that was still waiting for a thread when the server stopped is dropped.
    if (is_open)
    {
        ++current_threads;
        try
        {
            std::unique_ptr<Poco::Net::TCPServerConnection> connection(factory->createConnection(connection_socket, *this));
            if (connection)
                connection->run();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
        --current_threads;
    }
    /// The last access to `this`: the server is destroyed only when it has no connections.
    --current_connections;
}

}
