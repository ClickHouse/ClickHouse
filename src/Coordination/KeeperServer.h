#pragma once

#include <libnuraft/nuraft.hxx>
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/CoordinationSettings.h>
#include <base/logger_useful.h>

namespace DB
{

using RaftAppendResult = nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>;

class KeeperServer
{
private:
    const int server_id;

    CoordinationSettingsPtr coordination_settings;

    nuraft::ptr<KeeperStateMachine> state_machine;

    nuraft::ptr<KeeperStateManager> state_manager;

    nuraft::ptr<nuraft::raft_server> raft_instance;
    nuraft::ptr<nuraft::asio_service> asio_service;
    nuraft::ptr<nuraft::rpc_listener> asio_listener;

    std::mutex initialized_mutex;
    std::atomic<bool> initialized_flag = false;
    std::condition_variable initialized_cv;
    std::atomic<bool> initial_batch_committed = false;

    Poco::Logger * log;

    /// Callback func which is called by NuRaft on all internal events.
    /// Used to determine the moment when raft is ready to server new requests
    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    /// Almost copy-paste from nuraft::launcher, but with separated server init and start
    /// Allows to avoid race conditions.
    void launchRaftServer(
        bool enable_ipv6,
        const nuraft::raft_params & params,
        const nuraft::asio_service::options & asio_opts);

    void shutdownRaftServer();

public:
    KeeperServer(
        const KeeperConfigurationAndSettingsPtr & settings_,
        const Poco::Util::AbstractConfiguration & config_,
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_);

    /// Load state machine from the latest snapshot and load log storage. Start NuRaft with required settings.
    void startup(bool enable_ipv6 = true);

    /// Put local read request and execute in state machine directly and response into
    /// responses queue
    void putLocalReadRequest(const KeeperStorage::RequestForSession & request);

    /// Put batch of requests into Raft and get result of put. Responses will be set separately into
    /// responses_queue.
    RaftAppendResult putRequestBatch(const KeeperStorage::RequestsForSessions & requests);

    /// Return set of the non-active sessions
    std::vector<int64_t> getDeadSessions();

    nuraft::ptr<KeeperStateMachine> getKeeperStateMachine() const
    {
        return state_machine;
    }

    bool isLeader() const;

    bool isFollower() const;

    bool isObserver() const;

    bool isLeaderAlive() const;

    /// @return follower count if node is not leader return 0
    uint64_t getFollowerCount() const;

    /// @return synced follower count if node is not leader return 0
    uint64_t getSyncedFollowerCount() const;

    /// Wait server initialization (see callbackFunc)
    void waitInit();

    /// Return true if KeeperServer initialized
    bool checkInit() const
    {
        return initialized_flag;
    }

    void shutdown();

    int getServerID() const { return server_id; }

    /// Get configuration diff between current configuration in RAFT and in XML file
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config);

    /// Apply action for configuration update. Actually call raft_instance->remove_srv or raft_instance->add_srv.
    /// Synchronously check for update results with retries.
    void applyConfigurationUpdate(const ConfigUpdateAction & task);


    /// Wait configuration update for action. Used by followers.
    /// Return true if update was successfully received.
    bool waitConfigurationUpdate(const ConfigUpdateAction & task);
};

}
