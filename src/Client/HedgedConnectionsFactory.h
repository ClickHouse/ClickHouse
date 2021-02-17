#pragma once

#if defined(OS_LINUX)

#include <Common/TimerDescriptor.h>
#include <Common/Epoll.h>
#include <Common/FiberStack.h>
#include <Common/Fiber.h>
#include <Client/ConnectionEstablisher.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Core/Settings.h>
#include <unordered_map>
#include <memory>

namespace DB
{

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

    enum class State
    {
        READY,
        NOT_READY,
        CANNOT_CHOOSE,
    };

    struct ReplicaStatus
    {
        ReplicaStatus(ConnectionEstablisher connection_stablisher_) : connection_establisher(std::move(connection_stablisher_))
        {
        }

        ConnectionEstablisher connection_establisher;
        TimerDescriptor change_replica_timeout;
        bool is_ready = false;
    };

    HedgedConnectionsFactory(const ConnectionPoolWithFailoverPtr & pool_,
                        const Settings * settings_,
                        const ConnectionTimeouts & timeouts_,
                        std::shared_ptr<QualifiedTableName> table_to_check_ = nullptr);

    /// Create and return active connections according to pool_mode.
    std::vector<Connection *> getManyConnections(PoolMode pool_mode);

    /// Try to get connection to the new replica. If start_new_connection is true, we start establishing connection
    /// with the new replica. Process all current events in epoll (connections, timeouts),
    /// if there is no events in epoll and blocking is false, return NOT_READY.
    /// Returned state might be READY, NOT_READY and CANNOT_CHOOSE.
    /// If state is READY, replica connection will be written in connection_out.
    State getNextConnection(bool start_new_connection, bool blocking, Connection *& connection_out);

    /// Check if we can try to produce new READY replica.
//    bool canGetNewConnection() const { return ready_replicas_count + failed_pools_count < shuffled_pools.size(); }

    /// Stop working with all replicas that are not READY.
    void stopChoosingReplicas();

    bool hasEventsInProcess() const { return !epoll.empty(); }

    int getFileDescriptor() const { return epoll.getFileDescriptor(); }

    const ConnectionTimeouts & getConnectionTimeouts() const { return timeouts; }

    ~HedgedConnectionsFactory();

private:
    /// Try to start establishing connection to the new replica. Return
    /// the index of the new replica or -1 if cannot start new connection.
    int startEstablishingNewConnection(Connection *& connection_out);

    void processConnectionEstablisherStage(int replica_index, bool remove_from_epoll = false);

    /// Find an index of the next free replica to start connection.
    /// Return -1 if there is no free replica.
    int getNextIndex();

    int getReadyFileDescriptor(bool blocking);

    void processFailedConnection(int replica_index, bool remove_from_epoll);

    void processConnectionEstablisherEvent(int replica_index, Connection *& connection_out);

    void removeReplicaFromEpoll(int index);

    /// Return NOT_READY state if there is no ready events, READY if replica is ready
    /// and CANNOT_CHOOSE if there is no more events in epoll.
    State processEpollEvents(bool blocking, Connection *& connection_out);

    State setBestUsableReplica(Connection *& connection_out);

    const ConnectionPoolWithFailoverPtr pool;
    const Settings * settings;
    const ConnectionTimeouts timeouts;

    std::vector<ShuffledPool> shuffled_pools;
    std::vector<ReplicaStatus> replicas;

    /// Map socket file descriptor to replica index.
    std::unordered_map<int, int> fd_to_replica_index;

    /// Map timeout for changing replica to replica index.
    std::unordered_map<int, int> timeout_fd_to_replica_index;

    /// Indexes of replicas, that are in process of connection.
    size_t replicas_in_process_count = 0;
    /// Indexes of ready replicas.
    size_t ready_replicas_count = 0;

    std::shared_ptr<QualifiedTableName> table_to_check;
    int last_used_index = -1;
    bool fallback_to_stale_replicas;
    Epoll epoll;
    Poco::Logger * log;
    std::string fail_messages;
    size_t entries_count;
    size_t usable_count;
    size_t failed_pools_count;
    size_t max_tries;
};

}
#endif
