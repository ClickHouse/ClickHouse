#pragma once

#include <Common/TimerDescriptor.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Core/Settings.h>
#include <Common/Epoll.h>
#include <unordered_map>

namespace DB
{

/// Class for establishing hedged connections with replicas.
/// It works with multiple replicas simultaneously without blocking
/// (in current implementation only with 2 replicas) by using epoll.
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
        std::unordered_map<int, std::unique_ptr<TimerDescriptor>> active_timeouts;

        void reset()
        {
            connection = nullptr;
            state = State::EMPTY;
            index = -1;
            fd = -1;
            parallel_replica_offset = 0;
            active_timeouts.clear();
        }

        bool isReady() const { return state == State::READY; };
        bool isNotReady() const { return state == State::NOT_READY; };
        bool isEmpty() const { return state == State::EMPTY; };
        bool isCannotChoose() const { return state == State::CANNOT_CHOOSE; };
    };

    using ReplicaStatePtr = std::shared_ptr<ReplicaState>;


    struct Replicas
    {
        ReplicaStatePtr first_replica;
        ReplicaStatePtr second_replica;
    };

    GetHedgedConnections(const ConnectionPoolWithFailoverPtr & pool_,
                        const Settings * settings_,
                        const ConnectionTimeouts & timeouts_,
                        std::shared_ptr<QualifiedTableName> table_to_check_ = nullptr);

    std::vector<ReplicaStatePtr> getManyConnections(PoolMode pool_mode);

    ReplicaStatePtr getNextConnection(bool non_blocking);

    bool canGetNewConnection() const { return ready_indexes.size() + failed_pools_count < shuffled_pools.size(); }

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

    std::unordered_map<int, ReplicaStatePtr> fd_to_replica;
    std::unordered_map<int, ReplicaStatePtr> timeout_fd_to_replica;

//    std::vector<std::unique_ptr<ReplicaState>> replicas;
//    std::unordered_map<ReplicaStatePtr, std::unique_ptr<ReplicaState>> replicas_store;
//    ReplicaState first_replica;
//    ReplicaState second_replica;
    bool fallback_to_stale_replicas;
    Epoll epoll;
    Poco::Logger * log;
    std::string fail_messages;
    size_t entries_count;
    size_t usable_count;
    size_t failed_pools_count;
    size_t max_tries;
    int last_used_index;
    std::unordered_set<int> indexes_in_process;
    std::unordered_set<int> ready_indexes;

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
