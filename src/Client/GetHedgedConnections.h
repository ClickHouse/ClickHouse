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

/// Class for establishing hedged connections with replicas.
/// It works with multiple replicas simultaneously without blocking by using epoll.
class GetHedgedConnections
{
public:
    using ShuffledPool = ConnectionPoolWithFailover::Base::ShuffledPool;

    enum State
    {
        EMPTY = 0,
        READY = 1,
        NOT_READY = 2,
        CANNOT_CHOOSE = 3,
    };

    struct ReplicaState
    {
        Connection * connection = nullptr;
        State state = State::EMPTY;
        int index = -1;
        int fd = -1;
        size_t parallel_replica_offset = 0;
        std::unordered_map<int, std::shared_ptr<TimerDescriptor>> active_timeouts;

        void reset()
        {
            connection = nullptr;
            state = State::EMPTY;
            index = -1;
            fd = -1;
            parallel_replica_offset = 0;
            active_timeouts.clear();
        }

        bool isReady() const { return state == State::READY; }
        bool isNotReady() const { return state == State::NOT_READY; }
        bool isEmpty() const { return state == State::EMPTY; }
        bool isCannotChoose() const { return state == State::CANNOT_CHOOSE; }
    };

    using ReplicaStatePtr = std::shared_ptr<ReplicaState>;

    GetHedgedConnections(const ConnectionPoolWithFailoverPtr & pool_,
                        const Settings * settings_,
                        const ConnectionTimeouts & timeouts_,
                        std::shared_ptr<QualifiedTableName> table_to_check_ = nullptr);

    /// Create and return connections according to pool_mode.
    std::vector<ReplicaStatePtr> getManyConnections(PoolMode pool_mode);

    /// Try to establish connection to the new replica. If non_blocking is false, this function will block
    /// until establishing connection to the new replica (returned replica state might be READY or CANNOT_CHOOSE).
    /// If non_blocking is true, this function will try to establish connection to the new replica without blocking
    /// (returned replica state might be READY, NOT_READY and CANNOT_CHOOSE).
    ReplicaStatePtr getNextConnection(bool non_blocking);

    /// Check if we can try to produce new READY replica.
    bool canGetNewConnection() const { return ready_indexes.size() + failed_pools_count < shuffled_pools.size(); }

    /// Stop working with all replicas that are not READY.
    void stopChoosingReplicas();

    bool hasEventsInProcess() const { return epoll.size() > 0; }

    int getFileDescriptor() const { return epoll.getFileDescriptor(); }

    const ConnectionTimeouts & getConnectionTimeouts() const { return timeouts; }

    ~GetHedgedConnections();

private:

    enum Action
    {
        FINISH = 0,
        PROCESS_EPOLL_EVENTS = 1,
        TRY_NEXT_REPLICA = 2,
    };

    Action startTryGetConnection(int index, ReplicaStatePtr & replica);

    Action processTryGetConnectionStage(ReplicaStatePtr & replica, bool remove_from_epoll = false);

    /// Find an index of the next free replica to start connection.
    /// Return -1 if there is no free replica.
    int getNextIndex();

    int getReadyFileDescriptor(AsyncCallback async_callback = {});

    void addTimeouts(ReplicaStatePtr & replica);

    void processFailedConnection(ReplicaStatePtr & replica);

    void processReceiveTimeout(ReplicaStatePtr & replica);

    bool processReplicaEvent(ReplicaStatePtr & replica, bool non_blocking);

    bool processTimeoutEvent(ReplicaStatePtr & replica, TimerDescriptorPtr timeout_descriptor, bool non_blocking);

    ReplicaStatePtr processEpollEvents(bool non_blocking = false);

    void setBestUsableReplica(ReplicaStatePtr & replica);

    ReplicaStatePtr createNewReplica() { return std::make_shared<ReplicaState>(); }

    const ConnectionPoolWithFailoverPtr pool;
    const Settings * settings;
    const ConnectionTimeouts timeouts;
    std::shared_ptr<QualifiedTableName> table_to_check;

    std::vector<TryGetConnection> try_get_connections;
    std::vector<ShuffledPool> shuffled_pools;

    /// Map socket file descriptor to replica.
    std::unordered_map<int, ReplicaStatePtr> fd_to_replica;
    /// Map timeout file descriptor to replica.
    std::unordered_map<int, ReplicaStatePtr> timeout_fd_to_replica;

    /// Indexes of replicas, that are in process of connection.
    std::unordered_set<int> indexes_in_process;
    /// Indexes of ready replicas.
    std::unordered_set<int> ready_indexes;

    int last_used_index;
    bool fallback_to_stale_replicas;
    Epoll epoll;
    Poco::Logger * log;
    std::string fail_messages;
    size_t entries_count;
    size_t usable_count;
    size_t failed_pools_count;
    size_t max_tries;
};

/// Add timeout with particular type to replica and add it to epoll.
void addTimeoutToReplica(
    int type,
    GetHedgedConnections::ReplicaStatePtr & replica,
    Epoll & epoll,
    std::unordered_map<int, GetHedgedConnections::ReplicaStatePtr> & timeout_fd_to_replica,
    const ConnectionTimeouts & timeouts);

/// Remove timeout with particular type from replica and epoll.
void removeTimeoutFromReplica(
    int type,
    GetHedgedConnections::ReplicaStatePtr & replica,
    Epoll & epoll,
    std::unordered_map<int, GetHedgedConnections::ReplicaStatePtr> & timeout_fd_to_replica);

/// Remove all timeouts from replica and epoll.
void removeTimeoutsFromReplica(
    GetHedgedConnections::ReplicaStatePtr & replica,
    Epoll & epoll,
    std::unordered_map<int, GetHedgedConnections::ReplicaStatePtr> & timeout_fd_to_replica);

}
#endif
