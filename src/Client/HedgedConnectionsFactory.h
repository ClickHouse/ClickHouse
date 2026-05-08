#pragma once

#if defined(OS_LINUX)

#include <Common/TimerDescriptor.h>
#include <Common/Epoll.h>
#include <Common/FiberStack.h>
#include <Common/Fiber.h>
#include <Client/ConnectionEstablisher.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <unordered_map>
#include <memory>

namespace DB
{

struct Settings;

/** Class for establishing hedged connections with replicas.
  * The process of establishing connection is divided on stages, on each stage if
  * replica doesn't respond for a long time, we start establishing connection with
  * the next replica, without cancelling working with previous one.
  * It works with multiple replicas simultaneously without blocking by using epoll.
  */
class HedgedConnectionsFactory
{
public:
    using ShuffledPool = ConnectionPoolWithFailover::Base::ShuffledPool;
    using TryResult = PoolWithFailoverBase<IConnectionPool>::TryResult;

    enum class State : uint8_t
    {
        READY,
        NOT_READY,
        CANNOT_CHOOSE,
    };

    struct ReplicaStatus
    {
        explicit ReplicaStatus(std::unique_ptr<ConnectionEstablisherAsync> connection_stablisher_) : connection_establisher(std::move(connection_stablisher_))
        {
        }

        std::unique_ptr<ConnectionEstablisherAsync> connection_establisher;
        TimerDescriptor change_replica_timeout;
        bool is_ready = false;
    };

    HedgedConnectionsFactory(
        const ConnectionPoolWithFailoverPtr & pool_,
        const Settings & settings_,
        const ConnectionTimeouts & timeouts_,
        UInt64 max_tries_,
        bool fallback_to_stale_replicas_,
        UInt64 max_parallel_replicas_,
        bool skip_unavailable_shards_,
        std::shared_ptr<QualifiedTableName> table_to_check_ = nullptr,
        GetPriorityForLoadBalancing::Func priority_func = {});

    /// Create and return active connections according to pool_mode.
    std::vector<Connection *> getManyConnections(PoolMode pool_mode, AsyncCallback async_callback = {});

    /// Try to get connection to the new replica without blocking. Process all current events in epoll (connections, timeouts),
    /// Returned state might be READY (connection established successfully),
    /// NOT_READY (there are no ready events now) and CANNOT_CHOOSE (cannot produce new connection anymore).
    /// If state is READY, replica connection will be written in connection_out.
    State waitForReadyConnections(Connection *& connection_out);

    State startNewConnection(Connection *& connection_out);

    /// Stop working with all replicas that are not READY.
    void stopChoosingReplicas();

    bool hasEventsInProcess() const { return !epoll.empty(); }

    int getFileDescriptor() const { return epoll.getFileDescriptor(); }

    const ConnectionTimeouts & getConnectionTimeouts() const { return timeouts; }

    size_t numberOfProcessingReplicas() const;

    /// Tell Factory to not return connections with two level aggregation incompatibility.
    void skipReplicasWithTwoLevelAggregationIncompatibility() { skip_replicas_with_two_level_aggregation_incompatibility = true; }

    ~HedgedConnectionsFactory();

private:
    State waitForReadyConnectionsImpl(bool blocking, Connection *& connection_out, AsyncCallback & async_callback);

    /// Try to start establishing connection to the new replica. Return
    /// the index of the new replica or -1 if cannot start new connection.
    State startNewConnectionImpl(Connection *& connection_out);

    /// Find an index of the next free replica to start connection.
    /// Return -1 if there is no free replica.
    int getNextIndex();

    int getReadyFileDescriptor(bool blocking, AsyncCallback & async_callback);

    void processFailedConnection(int index, const std::string & fail_message);

    State resumeConnectionEstablisher(int index, Connection *& connection_out);

    State processFinishedConnection(int index, TryResult result, Connection *& connection_out);

    void removeReplicaFromEpoll(int index, int fd);

    void addNewReplicaToEpoll(int index, int fd);

    /// Return NOT_READY state if there is no ready events, READY if replica is ready
    /// and CANNOT_CHOOSE if there is no more events in epoll.
    State processEpollEvents(bool blocking, Connection *& connection_out, AsyncCallback & async_callback);

    State setBestUsableReplica(Connection *& connection_out);

    bool isTwoLevelAggregationIncompatible(Connection * connection);

    const ConnectionPoolWithFailoverPtr pool;
    const ConnectionTimeouts timeouts;

    std::vector<ShuffledPool> shuffled_pools;
    std::vector<ReplicaStatus> replicas;

    /// Map socket file descriptor to replica index.
    std::unordered_map<int, int> fd_to_replica_index;

    /// Map timeout for changing replica to replica index.
    std::unordered_map<int, int> timeout_fd_to_replica_index;

    /// If this flag is true, don't return connections with
    /// two level aggregation incompatibility
    bool skip_replicas_with_two_level_aggregation_incompatibility = false;

    std::shared_ptr<QualifiedTableName> table_to_check;
    int last_used_index = -1;
    Epoll epoll;
    LoggerPtr log;
    std::string fail_messages;

    /// The maximum number of attempts to connect to replicas.
    const size_t max_tries;
    const bool fallback_to_stale_replicas;
    /// Total number of established connections.
    size_t entries_count = 0;
    /// The number of established connections that are usable.
    size_t usable_count = 0;
    /// The number of established connections that are up to date.
    size_t up_to_date_count = 0;
    /// The number of failed connections (replica is considered failed after max_tries attempts to connect).
    size_t failed_pools_count= 0;

    /// The number of replicas that are in process of connection.
    size_t replicas_in_process_count = 0;
    /// The number of ready replicas (replica is considered ready when it's
    /// connection returns outside).
    size_t ready_replicas_count = 0;

    /// The number of requested in startNewConnection replicas (it's needed for
    /// checking the number of requested replicas that are still in process).
    size_t requested_connections_count = 0;

    const size_t max_parallel_replicas = 1;
    const bool skip_unavailable_shards = false;
};

}
#endif
