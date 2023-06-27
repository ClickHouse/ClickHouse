#include <QueryPipeline/ConnectionCollector.h>

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include "Core/Protocol.h"
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
extern const Metric AsyncDrainedConnections;
extern const Metric ActiveAsyncDrainedConnections;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

std::unique_ptr<ConnectionCollector> ConnectionCollector::connection_collector;

static constexpr UInt64 max_connection_draining_tasks_per_thread = 20;

ConnectionCollector::ConnectionCollector(ContextMutablePtr global_context_, size_t max_threads)
    : WithMutableContext(global_context_), pool(max_threads, max_threads, max_threads * max_connection_draining_tasks_per_thread)
{
}

ConnectionCollector & ConnectionCollector::init(ContextMutablePtr global_context_, size_t max_threads)
{
    if (connection_collector)
    {
        throw Exception("Connection collector is initialized twice. This is a bug", ErrorCodes::LOGICAL_ERROR);
    }

    connection_collector.reset(new ConnectionCollector(global_context_, max_threads));
    return *connection_collector;
}

struct AsyncDrainTask
{
    const ConnectionPoolWithFailoverPtr pool;
    std::shared_ptr<IConnections> shared_connections;
    void operator()() const
    {
        ConnectionCollector::drainConnections(*shared_connections, /* throw_error= */ false);
    }

    // We don't have std::unique_function yet. Wrap it in shared_ptr to make the functor copyable.
    std::shared_ptr<CurrentMetrics::Increment> metric_increment
        = std::make_shared<CurrentMetrics::Increment>(CurrentMetrics::ActiveAsyncDrainedConnections);
};

std::shared_ptr<IConnections> ConnectionCollector::enqueueConnectionCleanup(
    const ConnectionPoolWithFailoverPtr & pool, std::shared_ptr<IConnections> connections) noexcept
{
    if (!connections)
        return nullptr;

    if (connection_collector)
    {
        if (connection_collector->pool.trySchedule(AsyncDrainTask{pool, connections}))
        {
            CurrentMetrics::add(CurrentMetrics::AsyncDrainedConnections, 1);
            return nullptr;
        }
    }
    return connections;
}

void ConnectionCollector::drainConnections(IConnections & connections, bool throw_error)
{
    bool is_drained = false;
    try
    {
        Packet packet = connections.drain();
        is_drained = true;
        switch (packet.type)
        {
            case Protocol::Server::EndOfStream:
            case Protocol::Server::Log:
            case Protocol::Server::ProfileEvents:
                break;

            case Protocol::Server::Exception:
                packet.exception->rethrow();
                break;

            default:
                /// Connection should be closed in case of unexpected packet,
                /// since this means that the connection in some bad state.
                is_drained = false;
                throw NetException(
                    ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                    "Unexpected packet {} from one of the following replicas: {}. (expected EndOfStream, Log, ProfileEvents or Exception)",
                    Protocol::Server::toString(packet.type),
                    connections.dumpAddresses());
        }
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::get("ConnectionCollector"), __PRETTY_FUNCTION__);
        if (!is_drained)
        {
            try
            {
                connections.disconnect();
            }
            catch (...)
            {
                tryLogCurrentException(&Poco::Logger::get("ConnectionCollector"), __PRETTY_FUNCTION__);
            }
        }

        if (throw_error)
            throw;
    }
}

}
