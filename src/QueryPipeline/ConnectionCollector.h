#pragma once

#include <Client/IConnections.h>
#include <Interpreters/Context_fwd.h>
#include <boost/noncopyable.hpp>
#include <Common/ThreadPool.h>

namespace DB
{

class ConnectionPoolWithFailover;
using ConnectionPoolWithFailoverPtr = std::shared_ptr<ConnectionPoolWithFailover>;

class ConnectionCollector : boost::noncopyable, WithMutableContext
{
public:
    static ConnectionCollector & init(ContextMutablePtr global_context_, size_t max_threads);
    static std::shared_ptr<IConnections>
    enqueueConnectionCleanup(const ConnectionPoolWithFailoverPtr & pool, std::shared_ptr<IConnections> connections) noexcept;
    static void drainConnections(IConnections & connections, bool throw_error);

private:
    explicit ConnectionCollector(ContextMutablePtr global_context_, size_t max_threads);

    static constexpr size_t reschedule_time_ms = 1000;
    ThreadPool pool;
    static std::unique_ptr<ConnectionCollector> connection_collector;
};

}
