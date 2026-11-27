#include <Server/TCPServer.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

#include <Poco/Exception.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Timespan.h>

#include <optional>


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

    /// A failed join terminates: the accept thread uses this object.
    SCOPE_EXIT({
        if (accepting.exchange(false))
            accept_thread.join();

        // This notifies already established connections that they should stop serving
        // queries and close their socket as soon as they can.
        is_open = false;

        /// Dropped: the connections of other servers can hold every thread until a later phase of the shutdown.
        std::deque<Poco::Net::StreamSocket> waiting_sockets;
        {
            std::lock_guard lock(waiting_connections->mutex);
            waiting_sockets.swap(waiting_connections->sockets);
        }
        current_connections -= waiting_sockets.size();

        // Stopping the accept thread leaves the socket open.
        // To be able to hand over control of the listening port to a new server, and
        // to get fast connection refusal instead of timeouts, we also need to close
        // the listening socket.
        socket.close();
    });

    // FIXME: On Darwin and illumos, shutdown(SHUT_RD) on a listening socket
    // returns ENOTCONN, so accept() is not explicitly awakened.
#if !defined(OS_DARWIN) && !defined(OS_SUNOS)
    // Shutdown the listen socket before stopping tcp server to avoid 2.5second delay
    try
    {
        socket.shutdownReceive();
    }
    catch (...)
    {
        /// Only an early wake-up: the accept thread also stops within its 250 ms poll.
        tryLogCurrentException(log, "Cannot shut down the listening socket", LogsLevel::warning);
    }
#endif
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

                    /// Created before the connection is queued, because creating it may throw.
                    TCPServerThreadPool::Job job = [this, waiting = waiting_connections]
                    {
                        std::optional<Poco::Net::StreamSocket> waiting_socket;
                        {
                            std::lock_guard lock(waiting->mutex);
                            if (waiting->sockets.empty())
                                return;
                            waiting_socket = std::move(waiting->sockets.front());
                            waiting->sockets.pop_front();
                        }
                        serveConnection(*waiting_socket);
                    };

                    bool queued = false;
                    {
                        std::lock_guard lock(waiting_connections->mutex);
                        if (waiting_connections->sockets.size() < max_queued)
                        {
                            waiting_connections->sockets.push_back(connection_socket);
                            ++current_connections;
                            queued = true;
                        }
                    }

                    if (!queued)
                    {
                        ++refused_connections;
                        LOG_WARNING(log, "Refusing connection from {}, reached max queue size {}",
                            connection_socket.peerAddress().toString(), max_queued);
                        continue;
                    }

                    if (!thread_pool.trySchedule(std::move(job)))
                    {
                        /// The newest connection: the scheduled jobs take at most all the others.
                        {
                            std::lock_guard lock(waiting_connections->mutex);
                            waiting_connections->sockets.pop_back();
                        }
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
    /// The server can have stopped since the connection was taken from the queue.
    if (is_open)
    {
        ++current_threads;
        try
        {
            /// Per connection: the state of one connection, such as memory flushed without a limit check, must not reach the next one.
            ThreadStatus thread_status;
            const auto & global_pool = GlobalThreadPool::instance();
            if (unlikely(global_pool.global_profiler_real_time_period_ns != 0 || global_pool.global_profiler_cpu_time_period_ns != 0))
                thread_status.initGlobalProfiler(
                    global_pool.global_profiler_real_time_period_ns, global_pool.global_profiler_cpu_time_period_ns);

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
