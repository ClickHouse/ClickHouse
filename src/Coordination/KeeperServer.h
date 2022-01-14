#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/CoordinationSettings.h>
#include <unordered_map>
#include <common/logger_useful.h>

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

    std::mutex append_entries_mutex;

    std::mutex initialized_mutex;
    std::atomic<bool> initialized_flag = false;
    std::condition_variable initialized_cv;
    std::atomic<bool> initial_batch_committed = false;

    Poco::Logger * log;

    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    /// Almost copy-paste from nuraft::launcher, but with separated server init and start
    /// Allows to avoid race conditions.
    void launchRaftServer(
        const nuraft::raft_params & params,
        const nuraft::asio_service::options & asio_opts);

    void shutdownRaftServer();

public:
    KeeperServer(
        int server_id_,
        const CoordinationSettingsPtr & coordination_settings_,
        const Poco::Util::AbstractConfiguration & config,
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_,
        bool standalone_keeper);

    void startup();

    void putLocalReadRequest(const KeeperStorage::RequestForSession & request);

    RaftAppendResult putRequestBatch(const KeeperStorage::RequestsForSessions & requests);

    /// Return set of the non-active sessions
    std::vector<int64_t> getDeadSessions();

    bool isLeader() const;

    bool isLeaderAlive() const;

    void waitInit();

    void shutdown();

    int getServerID() const { return server_id; }
};

}
