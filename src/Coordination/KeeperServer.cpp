#include <Coordination/Defines.h>
#include <Coordination/KeeperServer.h>

#include "config_core.h"

#include <chrono>
#include <filesystem>
#include <string>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string.hpp>
#include <libnuraft/cluster_config.hxx>
#include <libnuraft/log_val_type.hxx>
#include <libnuraft/ptr.hxx>
#include <libnuraft/raft_server.hxx>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
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

std::string checkAndGetSuperdigest(const String & user_and_digest)
{
    if (user_and_digest.empty())
        return "";

    std::vector<std::string> scheme_and_id;
    boost::split(scheme_and_id, user_and_digest, [](char c) { return c == ':'; });
    if (scheme_and_id.size() != 2 || scheme_and_id[0] != "super")
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect superdigest in keeper_server config. Must be 'super:base64string'");

    return user_and_digest;
}

int32_t getValueOrMaxInt32AndLogWarning(uint64_t value, const std::string & name, Poco::Logger * log)
{
    if (value > std::numeric_limits<int32_t>::max())
    {
        LOG_WARNING(
            log,
            "Got {} value for setting '{}' which is bigger than int32_t max value, lowering value to {}.",
            value,
            name,
            std::numeric_limits<int32_t>::max());
        return std::numeric_limits<int32_t>::max();
    }

    return static_cast<int32_t>(value);
}

}

KeeperServer::KeeperServer(
    const KeeperConfigurationAndSettingsPtr & configuration_and_settings_,
    const Poco::Util::AbstractConfiguration & config,
    ResponsesQueue & responses_queue_,
    SnapshotsQueue & snapshots_queue_)
    : server_id(configuration_and_settings_->server_id)
    , coordination_settings(configuration_and_settings_->coordination_settings)
    , log(&Poco::Logger::get("KeeperServer"))
    , is_recovering(config.getBool("keeper_server.force_recovery", false))
    , keeper_context{std::make_shared<KeeperContext>()}
    , create_snapshot_on_exit(config.getBool("keeper_server.create_snapshot_on_exit", true))
{
    if (coordination_settings->quorum_reads)
        LOG_WARNING(log, "Quorum reads enabled, Keeper will work slower.");

    keeper_context->digest_enabled = config.getBool("keeper_server.digest_enabled", false);
    keeper_context->ignore_system_path_on_startup = config.getBool("keeper_server.ignore_system_path_on_startup", false);

    state_machine = nuraft::cs_new<KeeperStateMachine>(
        responses_queue_,
        snapshots_queue_,
        configuration_and_settings_->snapshot_storage_path,
        coordination_settings,
        keeper_context,
        checkAndGetSuperdigest(configuration_and_settings_->super_digest));

    state_manager = nuraft::cs_new<KeeperStateManager>(
        server_id,
        "keeper_server",
        configuration_and_settings_->log_storage_path,
        configuration_and_settings_->state_file_path,
        config,
        coordination_settings);
}

/**
 * Tiny wrapper around nuraft::raft_server which adds some functions
 * necessary for recovery, mostly connected to config manipulation.
 */
struct KeeperServer::KeeperRaftServer : public nuraft::raft_server
{
    bool isClusterHealthy()
    {
        if (timer_from_init)
        {
            size_t expiry = get_current_params().heart_beat_interval_ * raft_server::raft_limits_.response_limit_;

            if (timer_from_init->elapsedMilliseconds() < expiry)
                return false;

            timer_from_init.reset();
        }

        const size_t voting_members = get_num_voting_members();
        const auto not_responding_peers = get_not_responding_peers();
        const auto quorum_size = voting_members / 2 + 1;
        const auto max_not_responding_peers = voting_members - quorum_size;

        return not_responding_peers <= max_not_responding_peers;
    }

    // Manually set the internal config of the raft server
    // This should be used only for recovery
    void setConfig(const nuraft::ptr<nuraft::cluster_config> & new_config)
    {
        set_config(new_config);
    }

    // Manually reconfigure the cluster
    // This should be used only for recovery
    void forceReconfigure(const nuraft::ptr<nuraft::cluster_config> & new_config)
    {
        reconfigure(new_config);
    }

    using nuraft::raft_server::raft_server;

    // peers are initially marked as responding because at least one cycle
    // of heartbeat * response_limit (20) need to pass to be marked
    // as not responding
    // until that time passes we can't say that the cluster is healthy
    std::optional<Stopwatch> timer_from_init = std::make_optional<Stopwatch>();
};

void KeeperServer::loadLatestConfig()
{
    auto latest_snapshot_config = state_machine->getClusterConfig();
    auto latest_log_store_config = state_manager->getLatestConfigFromLogStore();

    if (latest_snapshot_config && latest_log_store_config)
    {
        if (latest_snapshot_config->get_log_idx() > latest_log_store_config->get_log_idx())
        {
            LOG_INFO(log, "Will use config from snapshot with log index {}", latest_snapshot_config->get_log_idx());
            state_manager->save_config(*latest_snapshot_config);
        }
        else
        {
            LOG_INFO(log, "Will use config from log store with log index {}", latest_snapshot_config->get_log_idx());
            state_manager->save_config(*latest_log_store_config);
        }
    }
    else if (latest_snapshot_config)
    {
        LOG_INFO(log, "No config in log store, will use config from snapshot with log index {}", latest_snapshot_config->get_log_idx());
        state_manager->save_config(*latest_snapshot_config);
    }
    else if (latest_log_store_config)
    {
        LOG_INFO(log, "No config in snapshot, will use config from log store with log index {}", latest_log_store_config->get_log_idx());
        state_manager->save_config(*latest_log_store_config);
    }
    else
    {
        LOG_INFO(log, "No config in log store and snapshot, probably it's initial run. Will use config from .xml on disk");
    }
}

void KeeperServer::enterRecoveryMode(nuraft::raft_params & params)
{
    LOG_WARNING(
        log,
        "This instance is in recovery mode. Until the quorum is restored, no requests should be sent to any "
        "of the cluster instances. This instance will start accepting requests only when the recovery is finished.");

    auto latest_config = state_manager->load_config();

    nuraft::ptr<nuraft::cluster_config> new_config = std::make_shared<nuraft::cluster_config>(0, latest_config ? latest_config->get_log_idx() : 0);
    new_config->set_log_idx(state_manager->load_log_store()->next_slot());

    new_config->get_servers() = last_local_config->get_servers();

    state_manager->save_config(*new_config);
    params.with_custom_commit_quorum_size(1);
    params.with_custom_election_quorum_size(1);
}

void KeeperServer::forceRecovery()
{
    // notify threads containing the lock that we want to enter recovery mode
    is_recovering = true;
    std::lock_guard lock{server_write_mutex};
    auto params = raft_instance->get_current_params();
    enterRecoveryMode(params);
    raft_instance->setConfig(state_manager->load_config());
    raft_instance->update_params(params);
}

void KeeperServer::launchRaftServer(bool enable_ipv6)
{
    nuraft::raft_params params;
    params.heart_beat_interval_
        = getValueOrMaxInt32AndLogWarning(coordination_settings->heart_beat_interval_ms.totalMilliseconds(), "heart_beat_interval_ms", log);
    params.election_timeout_lower_bound_ = getValueOrMaxInt32AndLogWarning(
        coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds(), "election_timeout_lower_bound_ms", log);
    params.election_timeout_upper_bound_ = getValueOrMaxInt32AndLogWarning(
        coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds(), "election_timeout_upper_bound_ms", log);
    params.reserved_log_items_ = getValueOrMaxInt32AndLogWarning(coordination_settings->reserved_log_items, "reserved_log_items", log);
    params.snapshot_distance_ = getValueOrMaxInt32AndLogWarning(coordination_settings->snapshot_distance, "snapshot_distance", log);

    if (params.snapshot_distance_ < 10000)
        LOG_WARNING(log, "Very small snapshot_distance {} specified in coordination settings. "
                    "It doesn't make sense to specify such small value, because it can lead to degraded performance and another issues.", params.snapshot_distance_);

    params.stale_log_gap_ = getValueOrMaxInt32AndLogWarning(coordination_settings->stale_log_gap, "stale_log_gap", log);
    params.fresh_log_gap_ = getValueOrMaxInt32AndLogWarning(coordination_settings->fresh_log_gap, "fresh_log_gap", log);
    params.client_req_timeout_
        = getValueOrMaxInt32AndLogWarning(coordination_settings->operation_timeout_ms.totalMilliseconds(), "operation_timeout_ms", log);
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    params.auto_forwarding_req_timeout_
        = std::max<uint64_t>(coordination_settings->operation_timeout_ms.totalMilliseconds() * 2, std::numeric_limits<int32_t>::max());
    params.auto_forwarding_req_timeout_
        = getValueOrMaxInt32AndLogWarning(coordination_settings->operation_timeout_ms.totalMilliseconds() * 2, "operation_timeout_ms", log);
    params.max_append_size_
        = getValueOrMaxInt32AndLogWarning(coordination_settings->max_requests_batch_size, "max_requests_batch_size", log);

    params.return_method_ = nuraft::raft_params::async_handler;

    nuraft::asio_service::options asio_opts{};
    if (state_manager->isSecure())
    {
#if USE_SSL
        setSSLParams(asio_opts);
#else
        throw Exception(
            "SSL support for NuRaft is disabled because ClickHouse was built without SSL support.", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }

    if (is_recovering)
        enterRecoveryMode(params);

    nuraft::raft_server::init_options init_options;

    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.start_server_in_constructor_ = false;
    init_options.raft_callback_ = [this](nuraft::cb_func::Type type, nuraft::cb_func::Param * param) { return callbackFunc(type, param); };

    nuraft::ptr<nuraft::logger> logger = nuraft::cs_new<LoggerWrapper>("RaftInstance", coordination_settings->raft_logs_level);
    asio_service = nuraft::cs_new<nuraft::asio_service>(asio_opts, logger);
    asio_listener = asio_service->create_rpc_listener(state_manager->getPort(), logger, enable_ipv6);

    if (!asio_listener)
        return;

    nuraft::ptr<nuraft::delayed_task_scheduler> scheduler = asio_service;
    nuraft::ptr<nuraft::rpc_client_factory> rpc_cli_factory = asio_service;

    nuraft::ptr<nuraft::state_mgr> casted_state_manager = state_manager;
    nuraft::ptr<nuraft::state_machine> casted_state_machine = state_machine;

    /// raft_server creates unique_ptr from it
    nuraft::context * ctx
        = new nuraft::context(casted_state_manager, casted_state_machine, asio_listener, logger, rpc_cli_factory, scheduler, params);

    raft_instance = nuraft::cs_new<KeeperRaftServer>(ctx, init_options);

    raft_instance->start_server(init_options.skip_initial_election_timeout_);

    nuraft::ptr<nuraft::raft_server> casted_raft_server = raft_instance;
    asio_listener->listen(casted_raft_server);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

void KeeperServer::startup(const Poco::Util::AbstractConfiguration & config, bool enable_ipv6)
{
    state_machine->init();

    state_manager->loadLogStore(state_machine->last_commit_index() + 1, coordination_settings->reserved_log_items);

    auto log_store = state_manager->load_log_store();
    auto next_log_idx = log_store->next_slot();
    if (next_log_idx > 0 && next_log_idx > state_machine->last_commit_index())
    {
        auto log_entries = log_store->log_entries(state_machine->last_commit_index() + 1, next_log_idx);

        LOG_INFO(log, "Preprocessing {} log entries", log_entries->size());
        auto idx = state_machine->last_commit_index() + 1;
        for (const auto & entry : *log_entries)
        {
            if (entry && entry->get_val_type() == nuraft::log_val_type::app_log)
                state_machine->pre_commit(idx, entry->get_buf());

            ++idx;
        }
    }

    loadLatestConfig();

    last_local_config = state_manager->parseServersConfiguration(config, true).cluster_config;

    launchRaftServer(enable_ipv6);

    keeper_context->server_state = KeeperContext::Phase::RUNNING;
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

    keeper_context->server_state = KeeperContext::Phase::SHUTDOWN;

    if (create_snapshot_on_exit)
        raft_instance->create_snapshot();

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
    state_manager->flushAndShutDownLogStore();
    shutdownRaftServer();
}

namespace
{

// Serialize the request with all the necessary information for the leader
// we don't know ZXID and digest yet so we don't serialize it
nuraft::ptr<nuraft::buffer> getZooKeeperRequestMessage(const KeeperStorage::RequestForSession & request_for_session)
{
    DB::WriteBufferFromNuraftBuffer write_buf;
    DB::writeIntBinary(request_for_session.session_id, write_buf);
    request_for_session.request->write(write_buf);
    DB::writeIntBinary(request_for_session.time, write_buf);
    return write_buf.getBuffer();
}

// Serialize the request for the log entry
nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(const KeeperStorage::RequestForSession & request_for_session)
{
    DB::WriteBufferFromNuraftBuffer write_buf;
    DB::writeIntBinary(request_for_session.session_id, write_buf);
    request_for_session.request->write(write_buf);
    DB::writeIntBinary(request_for_session.time, write_buf);
    DB::writeIntBinary(request_for_session.zxid, write_buf);
    assert(request_for_session.digest);
    DB::writeIntBinary(request_for_session.digest->version, write_buf);
    if (request_for_session.digest->version != KeeperStorage::DigestVersion::NO_DIGEST)
        DB::writeIntBinary(request_for_session.digest->value, write_buf);

    return write_buf.getBuffer();
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
    for (const auto & request_for_session : requests_for_sessions)
    {
        entries.push_back(getZooKeeperRequestMessage(request_for_session));
    }

    std::lock_guard lock{server_write_mutex};
    if (is_recovering)
        return nullptr;

    return raft_instance->append_entries(entries);
}

bool KeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}

bool KeeperServer::isObserver() const
{
    auto srv_config = state_manager->get_srv_config();
    return srv_config->is_learner();
}

bool KeeperServer::isFollower() const
{
    return !isLeader() && !isObserver();
}

bool KeeperServer::isLeaderAlive() const
{
    return raft_instance->is_leader_alive();
}

/// TODO test whether taking failed peer in count
uint64_t KeeperServer::getFollowerCount() const
{
    return raft_instance->get_peer_info_all().size();
}

uint64_t KeeperServer::getSyncedFollowerCount() const
{
    uint64_t last_log_idx = raft_instance->get_last_log_idx();
    const auto followers = raft_instance->get_peer_info_all();

    uint64_t stale_followers = 0;

    const uint64_t stale_follower_gap = raft_instance->get_current_params().stale_log_gap_;
    for (const auto & fl : followers)
    {
        if (last_log_idx > fl.last_log_idx_ + stale_follower_gap)
            stale_followers++;
    }
    return followers.size() - stale_followers;
}

nuraft::cb_func::ReturnCode KeeperServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param)
{
    if (is_recovering)
    {
        const auto finish_recovering = [&]
        {
            auto new_params = raft_instance->get_current_params();
            new_params.custom_commit_quorum_size_ = 0;
            new_params.custom_election_quorum_size_ = 0;
            raft_instance->update_params(new_params);

            LOG_INFO(log, "Recovery is done. You can continue using cluster normally.");
            is_recovering = false;
        };

        switch (type)
        {
            case nuraft::cb_func::HeartBeat:
            {
                if (raft_instance->isClusterHealthy())
                    finish_recovering();
                break;
            }
            case nuraft::cb_func::NewConfig:
            {
                // Apply the manually set config when in recovery mode
                // NuRaft will commit but skip the reconfigure if the current
                // config is the same as the committed one
                // Because we manually set the config to commit
                // we need to call the reconfigure also
                uint64_t log_idx = *static_cast<uint64_t *>(param->ctx);

                auto config = state_manager->load_config();
                if (log_idx == config->get_log_idx())
                {
                    raft_instance->forceReconfigure(config);

                    // Single node cluster doesn't need to wait for any other nodes
                    // so we can finish recovering immediately after applying
                    // new configuration
                    if (config->get_servers().size() == 1)
                        finish_recovering();
                }

                break;
            }
            case nuraft::cb_func::ProcessReq:
                // we don't accept requests from our peers or clients
                // while in recovery mode
                return nuraft::cb_func::ReturnCode::ReturnNull;
            default:
                break;
        }
    }

    if (initialized_flag)
    {
        switch (type)
        {
            // This event is called before a single log is appended to the entry on the leader node
            case nuraft::cb_func::PreAppendLog:
            {
                // we are relying on the fact that request are being processed under a mutex
                // and not a RW lock
                auto & entry = *static_cast<LogEntryPtr *>(param->ctx);

                assert(entry->get_val_type() == nuraft::app_log);
                auto next_zxid = state_machine->getNextZxid();

                auto & entry_buf = entry->get_buf();
                auto request_for_session = state_machine->parseRequest(entry_buf);
                request_for_session.zxid = next_zxid;
                state_machine->preprocess(request_for_session);
                request_for_session.digest = state_machine->getNodesDigest();
                entry = nuraft::cs_new<nuraft::log_entry>(entry->get_term(), getZooKeeperLogEntry(request_for_session), entry->get_val_type());
                break;
            }
            default:
                break;
        }

        return nuraft::cb_func::ReturnCode::Ok;
    }

    size_t last_commited = state_machine->last_commit_index();
    size_t next_index = state_manager->getLogStore()->next_slot();
    bool commited_store = false;
    if (next_index < last_commited || next_index - last_commited <= 1)
        commited_store = true;

    auto set_initialized = [this]()
    {
        std::lock_guard lock(initialized_mutex);
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

std::vector<int64_t> KeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

ConfigUpdateActions KeeperServer::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config)
{
    auto diff = state_manager->getConfigurationDiff(config);

    if (!diff.empty())
    {
        std::lock_guard lock{server_write_mutex};
        last_local_config = state_manager->parseServersConfiguration(config, true).cluster_config;
    }

    return diff;
}

void KeeperServer::applyConfigurationUpdate(const ConfigUpdateAction & task)
{
    std::lock_guard lock{server_write_mutex};
    if (is_recovering)
        return;

    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to add server with id {}", task.server->get_id());
        bool added = false;
        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added", task.server->get_id());
                added = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to add server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->add_srv(*task.server);
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to add server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!added)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to add server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                coordination_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to remove server with id {}", task.server->get_id());

        bool removed = false;
        if (task.server->get_id() == state_manager->server_id())
        {
            LOG_INFO(
                log,
                "Trying to remove leader node (ourself), so will yield leadership and some other node (new leader) will try remove us. "
                "Probably you will have to run SYSTEM RELOAD CONFIG on the new leader node");

            raft_instance->yield_leadership();
            return;
        }

        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed", task.server->get_id());
                removed = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to remove server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->remove_srv(task.server->get_id());
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to remove server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!removed)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to remove server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                coordination_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        raft_instance->set_priority(task.server->get_id(), task.server->get_priority());
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));
}


bool KeeperServer::waitConfigurationUpdate(const ConfigUpdateAction & task)
{
    if (is_recovering)
        return false;

    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to wait server with id {} to be added", task.server->get_id());
        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to add server {}", task.server->get_id());
                return false;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to wait remove of server with id {}", task.server->get_id());

        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to remove server {}", task.server->get_id());
                return false;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        return true;
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));
    return true;
}

Keeper4LWInfo KeeperServer::getPartiallyFilled4LWInfo() const
{
    Keeper4LWInfo result;
    result.is_leader = raft_instance->is_leader();

    auto srv_config = state_manager->get_srv_config();
    result.is_observer = srv_config->is_learner();

    result.is_follower = !result.is_leader && !result.is_observer;
    result.has_leader = result.is_leader || isLeaderAlive();
    result.is_standalone = !result.is_follower && getFollowerCount() == 0;
    if (result.is_leader)
    {
        result.follower_count = getFollowerCount();
        result.synced_follower_count = getSyncedFollowerCount();
    }
    result.total_nodes_count = getKeeperStateMachine()->getNodesCount();
    result.last_zxid = getKeeperStateMachine()->getLastProcessedZxid();
    return result;
}

}
