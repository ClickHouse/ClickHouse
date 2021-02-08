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

enum class ConnectionTimeoutType
{
    RECEIVE_HELLO_TIMEOUT,
    RECEIVE_TABLES_STATUS_TIMEOUT,
    RECEIVE_DATA_TIMEOUT,
    RECEIVE_TIMEOUT,
};

struct ConnectionTimeoutDescriptor
{
    ConnectionTimeoutType type;
    TimerDescriptor timer;
};

using ConnectionTimeoutDescriptorPtr = std::shared_ptr<ConnectionTimeoutDescriptor>;
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

    enum class State
    {
        EMPTY = 0,
        READY = 1,
        NOT_READY = 2,
        CANNOT_CHOOSE = 3,
    };

    struct ReplicaState
    {
        Connection * connection = nullptr;
        size_t index = -1;
        State state = State::EMPTY;
        std::unordered_map<int, ConnectionTimeoutDescriptorPtr> active_timeouts;

        void reset()
        {
            connection = nullptr;
            index = -1;
            state = State::EMPTY;
            active_timeouts.clear();
        }
    };

    using ReplicaStatePtr = std::shared_ptr<ReplicaState>;

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
    bool canGetNewConnection() const { return ready_indexes.size() + failed_pools_count < shuffled_pools.size(); }

    /// Stop working with all replicas that are not READY.
    void stopChoosingReplicas();

    bool hasEventsInProcess() const { return epoll.size() > 0; }

    int getFileDescriptor() const { return epoll.getFileDescriptor(); }

    const ConnectionTimeouts & getConnectionTimeouts() const { return timeouts; }

    ~HedgedConnectionsFactory();

private:
    ReplicaStatePtr startEstablishingConnection(int index);

    void processConnectionEstablisherStage(ReplicaStatePtr replica, bool remove_from_epoll = false);

    /// Find an index of the next free replica to start connection.
    /// Return -1 if there is no free replica.
    int getNextIndex();

    int getReadyFileDescriptor(bool blocking);

    void addTimeouts(ReplicaStatePtr replica);

    void addTimeoutToReplica(ConnectionTimeoutType type, ReplicaStatePtr replica);

    void removeTimeoutsFromReplica(ReplicaStatePtr replica);

    void processFailedConnection(ReplicaStatePtr replica);

    void processReceiveTimeout(ReplicaStatePtr replica);

    void processReplicaEvent(ReplicaStatePtr replica);

    void processTimeoutEvent(ReplicaStatePtr replica, ConnectionTimeoutDescriptorPtr timeout_descriptor);

    /// Return false if there is no ready events, return true if replica is ready
    /// or we need to try next replica.
    bool processEpollEvents(ReplicaStatePtr & replica, bool blocking);

    void setBestUsableReplica(ReplicaStatePtr replica);

    ReplicaStatePtr createNewReplica() { return std::make_shared<ReplicaState>(); }

    const ConnectionPoolWithFailoverPtr pool;
    const Settings * settings;
    const ConnectionTimeouts timeouts;
    std::shared_ptr<QualifiedTableName> table_to_check;

    std::vector<ConnectionEstablisher> connection_establishers;
    std::vector<ShuffledPool> shuffled_pools;

    /// Map socket file descriptor to replica.
    std::unordered_map<int, ReplicaStatePtr> fd_to_replica;
    /// Map timeout file descriptor to replica.
    std::unordered_map<int, ReplicaStatePtr> timeout_fd_to_replica;

    /// Indexes of replicas, that are in process of connection.
    std::unordered_set<int> indexes_in_process;
    /// Indexes of ready replicas.
    std::unordered_set<int> ready_indexes;

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

/// Create ConnectionTimeoutDescriptor with particular type.
ConnectionTimeoutDescriptorPtr createConnectionTimeoutDescriptor(ConnectionTimeoutType type, const ConnectionTimeouts & timeouts);

}
#endif
