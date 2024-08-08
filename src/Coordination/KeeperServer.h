#pragma once

#include <Coordination/InMemoryLogStore.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/KeeperStorage.h>
#include <libnuraft/raft_params.hxx>
#include <libnuraft/raft_server.hxx>
#include <Poco/Util/AbstractConfiguration.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/RaftServerConfig.h>

namespace DB
{

using RaftAppendResult = nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>;

struct KeeperConfigurationAndSettings;
using KeeperConfigurationAndSettingsPtr = std::shared_ptr<KeeperConfigurationAndSettings>;

class KeeperServer
{
private:
    const int server_id;

    nuraft::ptr<IKeeperStateMachine> state_machine;

    nuraft::ptr<KeeperStateManager> state_manager;

    struct KeeperRaftServer;
    nuraft::ptr<KeeperRaftServer> raft_instance; // TSA_GUARDED_BY(server_write_mutex);
    nuraft::ptr<nuraft::asio_service> asio_service;
    std::vector<nuraft::ptr<nuraft::rpc_listener>> asio_listeners;

    // because some actions can be applied
    // when we are sure that there are no requests currently being
    // processed (e.g. recovery) we do all write actions
    // on raft_server under this mutex.
    mutable std::mutex server_write_mutex;

    std::mutex initialized_mutex;
    std::atomic<bool> initialized_flag = false;
    std::condition_variable initialized_cv;
    std::atomic<bool> initial_batch_committed = false;

    std::atomic<uint64_t> last_log_idx_on_disk = 0;

    nuraft::ptr<nuraft::cluster_config> last_local_config;

    LoggerPtr log;

    /// Callback func which is called by NuRaft on all internal events.
    /// Used to determine the moment when raft is ready to server new requests
    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    /// Almost copy-paste from nuraft::launcher, but with separated server init and start
    /// Allows to avoid race conditions.
    void launchRaftServer(const Poco::Util::AbstractConfiguration & config, bool enable_ipv6);

    void shutdownRaftServer();

    void loadLatestConfig();

    void enterRecoveryMode(nuraft::raft_params & params);

    std::atomic_bool is_recovering = false;

    KeeperContextPtr keeper_context;

    const bool create_snapshot_on_exit;
    const bool enable_reconfiguration;

public:
    KeeperServer(
        const KeeperConfigurationAndSettingsPtr & settings_,
        const Poco::Util::AbstractConfiguration & config_,
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_,
        KeeperContextPtr keeper_context_,
        KeeperSnapshotManagerS3 & snapshot_manager_s3,
        IKeeperStateMachine::CommitCallback commit_callback);

    /// Load state machine from the latest snapshot and load log storage. Start NuRaft with required settings.
    void startup(const Poco::Util::AbstractConfiguration & config, bool enable_ipv6 = true);

    /// Put local read request and execute in state machine directly and response into
    /// responses queue
    void putLocalReadRequest(const KeeperStorageBase::RequestForSession & request);

    bool isRecovering() const { return is_recovering; }
    bool reconfigEnabled() const { return enable_reconfiguration; }

    /// Put batch of requests into Raft and get result of put. Responses will be set separately into
    /// responses_queue.
    RaftAppendResult putRequestBatch(const KeeperStorageBase::RequestsForSessions & requests);

    /// Return set of the non-active sessions
    std::vector<int64_t> getDeadSessions();

    nuraft::ptr<IKeeperStateMachine> getKeeperStateMachine() const { return state_machine; }

    void forceRecovery();

    bool isLeader() const;

    bool isFollower() const;

    bool isObserver() const;

    bool isLeaderAlive() const;

    bool isExceedingMemorySoftLimit() const;

    Keeper4LWInfo getPartiallyFilled4LWInfo() const;

    /// @return follower count if node is not leader return 0
    uint64_t getFollowerCount() const;

    /// @return synced follower count if node is not leader return 0
    uint64_t getSyncedFollowerCount() const;

    /// Wait server initialization (see callbackFunc)
    void waitInit();

    /// Return true if KeeperServer initialized
    bool checkInit() const { return initialized_flag; }

    void shutdown();

    int getServerID() const { return server_id; }

    enum class ConfigUpdateState : uint8_t
    {
        Accepted,
        Declined,
        WaitBeforeChangingLeader
    };

    ConfigUpdateState applyConfigUpdate(
        const ClusterUpdateAction& action,
        bool last_command_was_leader_change = false);

    // TODO (myrrc) these functions should be removed once "reconfig" is stabilized
    void applyConfigUpdateWithReconfigDisabled(const ClusterUpdateAction& action);
    bool waitForConfigUpdateWithReconfigDisabled(const ClusterUpdateAction& action);
    ClusterUpdateActions getRaftConfigurationDiff(const Poco::Util::AbstractConfiguration & config);

    uint64_t createSnapshot();

    KeeperLogInfo getKeeperLogInfo();

    bool requestLeader();

    void yieldLeadership();

    void recalculateStorageStats();
};

}
