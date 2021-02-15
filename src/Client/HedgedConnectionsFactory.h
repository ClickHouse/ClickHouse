#pragma once

#if defined(OS_LINUX)

#include <Common/TimerDescriptor.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Core/Settings.h>
#include <Common/Epoll.h>
#include <unordered_map>
#include <memory>

namespace DB
{

using TimerDescriptorPtr = std::shared_ptr<TimerDescriptor>;

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

    struct ReplicaStatus
    {
        ReplicaStatus(const ConnectionEstablisher & establisher) : connection_establisher(establisher)
        {
            epoll.add(receive_timeout.getDescriptor());
            epoll.add(change_replica_timeout.getDescriptor());
        }

        ConnectionEstablisher connection_establisher;
        TimerDescriptor receive_timeout;
        TimerDescriptor change_replica_timeout;
        bool is_ready = false;
        bool is_in_process = false;
        Epoll epoll;
    };

    enum class State
    {
        READY,
        NOT_READY,
        CANNOT_CHOOSE,
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
    bool canGetNewConnection() const { return ready_replicas_count + failed_pools_count < shuffled_pools.size(); }

    /// Stop working with all replicas that are not READY.
    void stopChoosingReplicas();

    bool hasEventsInProcess() const { return epoll.size() > 0; }

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

    int checkPendingData();

    void addTimeouts(int replica_index);

    void resetReplicaTimeouts(int replica_index);

    void processFailedConnection(int replica_index, bool remove_from_epoll);

    void processSocketEvent(int replica_index, Connection *& connection_out);

    void processReceiveTimeout(int replica_index);

    /// Return NOT_READY state if there is no ready events, READY if replica is ready
    /// and CANNOT_CHOOSE if there is no more events in epoll.
    State processEpollEvents(bool blocking, Connection *& connection_out);

    State setBestUsableReplica(Connection *& connection_out);

    const ConnectionPoolWithFailoverPtr pool;
    const Settings * settings;
    const ConnectionTimeouts timeouts;
    std::shared_ptr<QualifiedTableName> table_to_check;

    std::vector<ReplicaStatus> replicas;
    std::vector<ShuffledPool> shuffled_pools;

    /// Map socket file descriptor to replica index.
    std::unordered_map<int, int> fd_to_replica_index;

    /// Indexes of replicas, that are in process of connection.
    size_t replicas_in_process_count = 0;
    /// Indexes of ready replicas.
    size_t ready_replicas_count = 0;

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
