#include <Coordination/KeeperServer.h>

#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#endif

#include <Coordination/LoggerWrapper.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <chrono>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <string>
#include <Poco/Util/Application.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
}

namespace
{

#if USE_SSL
void setSSLParams(nuraft::asio_service::options & asio_opts)
{
    const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
    String certificate_file_property = "openSSL.server.certificateFile";
    String private_key_file_property = "openSSL.server.privateKeyFile";
    String root_ca_file_property = "openSSL.server.caConfig";

    if (!config.has(certificate_file_property))
        throw Exception("Server certificate file is not set.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (!config.has(private_key_file_property))
        throw Exception("Server private key file is not set.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    asio_opts.enable_ssl_ = true;
    asio_opts.server_cert_file_ = config.getString(certificate_file_property);
    asio_opts.server_key_file_ = config.getString(private_key_file_property);

    if (config.has(root_ca_file_property))
        asio_opts.root_cert_file_ = config.getString(root_ca_file_property);

    if (config.getBool("openSSL.server.loadDefaultCAFile", false))
        asio_opts.load_default_ca_file_ = true;

    if (config.getString("openSSL.server.verificationMode", "none") == "none")
        asio_opts.skip_verification_ = true;
}
#endif

}

KeeperServer::KeeperServer(
    int server_id_,
    const CoordinationSettingsPtr & coordination_settings_,
    const Poco::Util::AbstractConfiguration & config,
    ResponsesQueue & responses_queue_,
    SnapshotsQueue & snapshots_queue_)
    : server_id(server_id_)
    , coordination_settings(coordination_settings_)
    , state_machine(nuraft::cs_new<KeeperStateMachine>(
                        responses_queue_, snapshots_queue_,
                        config.getString("keeper_server.snapshot_storage_path", config.getString("path", DBMS_DEFAULT_PATH) + "coordination/snapshots"),
                        coordination_settings))
    , state_manager(nuraft::cs_new<KeeperStateManager>(server_id, "keeper_server", config, coordination_settings))
    , log(&Poco::Logger::get("KeeperServer"))
{
    if (coordination_settings->quorum_reads)
        LOG_WARNING(log, "Quorum reads enabled, Keeper will work slower.");
}

void KeeperServer::startup()
{
    state_machine->init();

    state_manager->loadLogStore(state_machine->last_commit_index() + 1, coordination_settings->reserved_log_items);

    bool single_server = state_manager->getTotalServers() == 1;

    nuraft::raft_params params;
    if (single_server)
    {
        /// Don't make sense in single server mode
        params.heart_beat_interval_ = 0;
        params.election_timeout_lower_bound_ = 0;
        params.election_timeout_upper_bound_ = 0;
    }
    else
    {
        params.heart_beat_interval_ = coordination_settings->heart_beat_interval_ms.totalMilliseconds();
        params.election_timeout_lower_bound_ = coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds();
        params.election_timeout_upper_bound_ = coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds();
    }

    params.reserved_log_items_ = coordination_settings->reserved_log_items;
    params.snapshot_distance_ = coordination_settings->snapshot_distance;
    params.stale_log_gap_ = coordination_settings->stale_log_gap;
    params.fresh_log_gap_ = coordination_settings->fresh_log_gap;
    params.client_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds();
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    params.auto_forwarding_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds() * 2;

    params.return_method_ = nuraft::raft_params::async_handler;

    nuraft::asio_service::options asio_opts{};
    if (state_manager->isSecure())
    {
#if USE_SSL
        setSSLParams(asio_opts);
#else
        throw Exception{"SSL support for NuRaft is disabled because ClickHouse was built without SSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }

    launchRaftServer(params, asio_opts);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

void KeeperServer::launchRaftServer(
    const nuraft::raft_params & params,
    const nuraft::asio_service::options & asio_opts)
{
    nuraft::raft_server::init_options init_options;

    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.start_server_in_constructor_ = false;
    init_options.raft_callback_ = [this] (nuraft::cb_func::Type type, nuraft::cb_func::Param * param)
    {
        return callbackFunc(type, param);
    };

    nuraft::ptr<nuraft::logger> logger = nuraft::cs_new<LoggerWrapper>("RaftInstance", coordination_settings->raft_logs_level);
    asio_service = nuraft::cs_new<nuraft::asio_service>(asio_opts, logger);
    asio_listener = asio_service->create_rpc_listener(state_manager->getPort(), logger);

    if (!asio_listener)
        return;

    nuraft::ptr<nuraft::delayed_task_scheduler> scheduler = asio_service;
    nuraft::ptr<nuraft::rpc_client_factory> rpc_cli_factory = asio_service;

    nuraft::ptr<nuraft::state_mgr> casted_state_manager = state_manager;
    nuraft::ptr<nuraft::state_machine> casted_state_machine = state_machine;

    /// raft_server creates unique_ptr from it
    nuraft::context * ctx = new nuraft::context(
        casted_state_manager, casted_state_machine,
        asio_listener, logger, rpc_cli_factory, scheduler, params);

    raft_instance = nuraft::cs_new<nuraft::raft_server>(ctx, init_options);

    raft_instance->start_server(init_options.skip_initial_election_timeout_);
    asio_listener->listen(raft_instance);
}

void KeeperServer::shutdownRaftServer()
{
    size_t timeout = coordination_settings->shutdown_timeout.totalSeconds();

    if (!raft_instance)
    {
        LOG_INFO(log, "RAFT doesn't start, shutdown not required");
        return;
    }

    raft_instance->shutdown();
    raft_instance.reset();

    if (asio_listener)
    {
        asio_listener->stop();
        asio_listener->shutdown();
    }

    if (asio_service)
    {
        asio_service->stop();
        size_t count = 0;
        while (asio_service->get_active_workers() != 0 && count < timeout * 100)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            count++;
        }
    }

    if (asio_service->get_active_workers() != 0)
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", timeout);
}


void KeeperServer::shutdown()
{
    state_machine->shutdownStorage();
    state_manager->flushLogStore();
    shutdownRaftServer();
}

namespace
{

nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::WriteBufferFromNuraftBuffer buf;
    DB::writeIntBinary(session_id, buf);
    request->write(buf);
    return buf.getBuffer();
}

}


void KeeperServer::putLocalReadRequest(const KeeperStorage::RequestForSession & request_for_session)
{
    if (!request_for_session.request->isReadRequest())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot process non-read request locally");

    state_machine->processReadRequest(request_for_session);
}

RaftAppendResult KeeperServer::putRequestBatch(const KeeperStorage::RequestsForSessions & requests_for_sessions)
{

    std::vector<nuraft::ptr<nuraft::buffer>> entries;
    for (const auto & [session_id, request] : requests_for_sessions)
        entries.push_back(getZooKeeperLogEntry(session_id, request));

    {
        std::lock_guard lock(append_entries_mutex);
        return raft_instance->append_entries(entries);
    }
}

bool KeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}

bool KeeperServer::isLeaderAlive() const
{
    return raft_instance->is_leader_alive();
}

nuraft::cb_func::ReturnCode KeeperServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param)
{
    if (initialized_flag)
        return nuraft::cb_func::ReturnCode::Ok;

    size_t last_commited = state_machine->last_commit_index();
    size_t next_index = state_manager->getLogStore()->next_slot();
    bool commited_store = false;
    if (next_index < last_commited || next_index - last_commited <= 1)
        commited_store = true;

    auto set_initialized = [this] ()
    {
        std::unique_lock lock(initialized_mutex);
        initialized_flag = true;
        initialized_cv.notify_all();
    };

    switch (type)
    {
        case nuraft::cb_func::BecomeLeader:
        {
            /// We become leader and store is empty or we already committed it
            if (commited_store || initial_batch_committed)
                set_initialized();
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::BecomeFollower:
        case nuraft::cb_func::GotAppendEntryReqFromLeader:
        {
            if (param->leaderId != -1)
            {
                auto leader_index = raft_instance->get_leader_committed_log_idx();
                auto our_index = raft_instance->get_committed_log_idx();
                /// This may happen when we start RAFT cluster from scratch.
                /// Node first became leader, and after that some other node became leader.
                /// BecameFresh for this node will not be called because it was already fresh
                /// when it was leader.
                if (leader_index < our_index + coordination_settings->fresh_log_gap)
                    set_initialized();
            }
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::BecomeFresh:
        {
            set_initialized(); /// We are fresh follower, ready to serve requests.
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::InitialBatchCommited:
        {
            if (param->myId == param->leaderId) /// We have committed our log store and we are leader, ready to serve requests.
                set_initialized();
            initial_batch_committed = true;
            return nuraft::cb_func::ReturnCode::Ok;
        }
        default: /// ignore other events
            return nuraft::cb_func::ReturnCode::Ok;
    }
}

void KeeperServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = coordination_settings->startup_timeout.totalMilliseconds();
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::unordered_set<int64_t> KeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

}
