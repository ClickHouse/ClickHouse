#pragma once

#include <Common/Stopwatch.h>
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStateManager.h>
#include <libnuraft/raft_params.hxx>
#include <libnuraft/raft_server.hxx>
#include <Poco/Util/AbstractConfiguration.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/RaftServerConfig.h>

namespace DB
{

using RaftAppendResult = nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>;

struct KeeperConfiguration;
using KeeperConfigurationPtr = std::shared_ptr<KeeperConfiguration>;

class KeeperServer
{
public:
    struct RespondingCounts
    {
        uint64_t learners = 0;
        uint64_t followers = 0;
        uint64_t synced_followers = 0;
        uint64_t synced_non_voting_followers = 0;
    };

private:
    friend class KeeperRequestDispatcher;

    /**
     * Tiny wrapper around nuraft::raft_server which adds some functions
     * necessary for recovery, mostly connected to config manipulation.
     */
    struct KeeperRaftServer : public nuraft::raft_server
    {
        bool isClusterHealthy();

        // Manually set the internal config of the raft server
        // This should be used only for recovery
        void setConfig(const nuraft::ptr<nuraft::cluster_config> & new_config);

        // Manually reconfigure the cluster
        // This should be used only for recovery
        void forceReconfigure(const nuraft::ptr<nuraft::cluster_config> & new_config);

        void commit_in_bg() override;
        void append_entries_in_bg() override;

        std::unique_lock<std::recursive_mutex> lockRaft();

        bool isCommitInProgress() const;

        void setServingRequest(bool value);

        /// Collect IDs of learner (non-voting) servers from the cluster config.
        std::unordered_set<int32_t> getLearnerIds();

        /// Returns alive (responding) learner and follower counters.
        /// Follower counters include only voting peers; learners include all peers.
        /// Both get_peer_info_all and get_srv_config_all hold the raft lock internally.
        KeeperServer::RespondingCounts getRespondingCounts();

        using nuraft::raft_server::raft_server;

        /// Keeper context for accessing coordination settings (e.g. commit profiler).
        /// Set after construction because the base class constructor is inherited.
        KeeperContextPtr keeper_context;

        // peers are initially marked as responding because at least one cycle
        // of heartbeat * response_limit (20) need to pass to be marked
        // as not responding
        // until that time passes we can't say that the cluster is healthy
        std::optional<Stopwatch> timer_from_init = std::make_optional<Stopwatch>();
    };

    const int server_id;

    nuraft::ptr<IKeeperStateMachine> state_machine;

    nuraft::ptr<KeeperStateManager> state_manager;

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
    const bool is_standalone_keeper;

public:
    KeeperServer(
        const KeeperConfigurationPtr & server_config,
        const Poco::Util::AbstractConfiguration & config_,
        KeeperResponseCallback response_callback_,
        SnapshotsQueue & snapshots_queue_,
        KeeperContextPtr keeper_context_,
        KeeperSnapshotManagerS3 & snapshot_manager_s3,
        IKeeperStateMachine::CommitCallback commit_callback);

    /// Load state machine from the latest snapshot and load log storage. Start NuRaft with required settings.
    void startup(const Poco::Util::AbstractConfiguration & config, bool enable_ipv6 = true);

    /// Execute read requests directly in the local state machine. Put response into responses queue.
    void putLocalReadRequests(const KeeperRequestsForSessions & requests);

    bool isRecovering() const { return is_recovering; }
    bool reconfigEnabled() const { return enable_reconfiguration; }

    /// Put batch of requests into Raft and get result of put. Responses will be sent separately
    /// through response_callback.
    RaftAppendResult putRequestBatch(const KeeperRequestsForSessions & requests);

    /// Return set of the non-active sessions
    std::vector<int64_t> getDeadSessions();

    nuraft::ptr<IKeeperStateMachine> getKeeperStateMachine() const { return state_machine; }

    void forceRecovery();

    bool isLeader() const;

    bool isFollower() const;

    bool isObserver() const;

    bool isLeaderAlive() const;

    bool isExceedingMemorySoftLimit() const;

    int64_t getLeaderID() const;

    Keeper4LWInfo getPartiallyFilled4LWInfo() const;

    /// @return responding learners/followers/synced followers; all zero when node is not leader
    RespondingCounts getRespondingCounts() const;

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
        const ClusterUpdateAction & action,
        bool last_command_was_leader_change = false);

    // TODO (myrrc) these functions should be removed once "reconfig" is stabilized
    void applyConfigUpdateWithReconfigDisabled(const ClusterUpdateAction& action);
    bool waitForConfigUpdateWithReconfigDisabled(const ClusterUpdateAction& action);
    ClusterUpdateActions getRaftConfigurationDiff(const Poco::Util::AbstractConfiguration & config);

    uint64_t createSnapshot();

    KeeperLogInfo getKeeperLogInfo();

    std::vector<KeeperChangelogStatus> getChangelogsStatus() const;

    bool requestLeader();

    void yieldLeadership();

    void recalculateStorageStats();

    std::optional<AuthenticationData> getAuthenticationData() const { return state_manager->getAuthenticationData(); }

    const KeeperContextPtr & getKeeperContext() const { return keeper_context; }
};

}
