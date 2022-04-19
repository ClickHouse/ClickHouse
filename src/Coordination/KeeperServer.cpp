#include <Coordination/KeeperServer.h>
#include <Coordination/Defines.h>

#include "config_core.h"

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
#include <filesystem>
#include <Poco/Util/Application.h>
#include <boost/algorithm/string.hpp>

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
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect superdigest in keeper_server config. Must be 'super:base64string'");

    return user_and_digest;
}

int32_t getValueOrMaxInt32AndLogWarning(uint64_t value, const std::string & name, Poco::Logger * log)
{
    if (value > std::numeric_limits<int32_t>::max())
    {
        LOG_WARNING(log, "Got {} value for setting '{}' which is bigger than int32_t max value, lowering value to {}.", value, name, std::numeric_limits<int32_t>::max());
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
    , state_machine(nuraft::cs_new<KeeperStateMachine>(
                        responses_queue_, snapshots_queue_,
                        configuration_and_settings_->snapshot_storage_path,
                        coordination_settings,
                        checkAndGetSuperdigest(configuration_and_settings_->super_digest)))
    , state_manager(nuraft::cs_new<KeeperStateManager>(server_id, "keeper_server", configuration_and_settings_->log_storage_path, config, coordination_settings))
    , log(&Poco::Logger::get("KeeperServer"))
{
    if (coordination_settings->quorum_reads)
        LOG_WARNING(log, "Quorum reads enabled, Keeper will work slower.");
}

void KeeperServer::startup(bool enable_ipv6)
{
    state_machine->init();

    state_manager->loadLogStore(state_machine->last_commit_index() + 1, coordination_settings->reserved_log_items);

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

    nuraft::raft_params params;
    params.heart_beat_interval_ = getValueOrMaxInt32AndLogWarning(coordination_settings->heart_beat_interval_ms.totalMilliseconds(), "heart_beat_interval_ms", log);
    params.election_timeout_lower_bound_ = getValueOrMaxInt32AndLogWarning(coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds(), "election_timeout_lower_bound_ms", log);
    params.election_timeout_upper_bound_ = getValueOrMaxInt32AndLogWarning(coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds(), "election_timeout_upper_bound_ms", log);
    params.reserved_log_items_ = getValueOrMaxInt32AndLogWarning(coordination_settings->reserved_log_items, "reserved_log_items", log);
    params.snapshot_distance_ = getValueOrMaxInt32AndLogWarning(coordination_settings->snapshot_distance, "snapshot_distance", log);
    params.stale_log_gap_ = getValueOrMaxInt32AndLogWarning(coordination_settings->stale_log_gap, "stale_log_gap", log);
    params.fresh_log_gap_ = getValueOrMaxInt32AndLogWarning(coordination_settings->fresh_log_gap, "fresh_log_gap", log);
    params.client_req_timeout_ = getValueOrMaxInt32AndLogWarning(coordination_settings->operation_timeout_ms.totalMilliseconds(), "operation_timeout_ms", log);
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    params.auto_forwarding_req_timeout_ = std::max<uint64_t>(coordination_settings->operation_timeout_ms.totalMilliseconds() * 2, std::numeric_limits<int32_t>::max());
    params.auto_forwarding_req_timeout_ = getValueOrMaxInt32AndLogWarning(coordination_settings->operation_timeout_ms.totalMilliseconds() * 2, "operation_timeout_ms", log);
    params.max_append_size_ = getValueOrMaxInt32AndLogWarning(coordination_settings->max_requests_batch_size, "max_requests_batch_size", log);

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

    launchRaftServer(enable_ipv6, params, asio_opts);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

void KeeperServer::launchRaftServer(
    bool enable_ipv6,
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
    asio_listener = asio_service->create_rpc_listener(state_manager->getPort(), logger, enable_ipv6);

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

nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, int64_t time, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::WriteBufferFromNuraftBuffer buf;
    DB::writeIntBinary(session_id, buf);
    request->write(buf);
    DB::writeIntBinary(time, buf);
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
    for (const auto & [session_id, time, request] : requests_for_sessions)
        entries.push_back(getZooKeeperLogEntry(session_id, time, request));

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

std::vector<int64_t> KeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

ConfigUpdateActions KeeperServer::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config)
{
    return state_manager->getConfigurationDiff(config);
}

void KeeperServer::applyConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to add server with id {}", task.server->get_id());
        bool added = false;
        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count; ++i)
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
                LOG_INFO(log, "Command to add server {} was not accepted for the {} time, will sleep for {} ms and retry", task.server->get_id(), i + 1, sleep_ms * (i + 1));

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!added)
            throw Exception(ErrorCodes::RAFT_ERROR, "Configuration change to add server (id {}) was not accepted by RAFT after all {} retries", task.server->get_id(), coordination_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to remove server with id {}", task.server->get_id());

        bool removed = false;
        if (task.server->get_id() == state_manager->server_id())
        {
            LOG_INFO(log, "Trying to remove leader node (ourself), so will yield leadership and some other node (new leader) will try remove us. "
                        "Probably you will have to run SYSTEM RELOAD CONFIG on the new leader node");

            raft_instance->yield_leadership();
            return;
        }

        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count; ++i)
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
                LOG_INFO(log, "Command to remove server {} was not accepted for the {} time, will sleep for {} ms and retry", task.server->get_id(), i + 1, sleep_ms * (i + 1));

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!removed)
            throw Exception(ErrorCodes::RAFT_ERROR, "Configuration change to remove server (id {}) was not accepted by RAFT after all {} retries", task.server->get_id(), coordination_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        raft_instance->set_priority(task.server->get_id(), task.server->get_priority());
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));
}


bool KeeperServer::waitConfigurationUpdate(const ConfigUpdateAction & task)
{

    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to wait server with id {} to be added", task.server->get_id());
        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count; ++i)
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

        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count; ++i)
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

}
