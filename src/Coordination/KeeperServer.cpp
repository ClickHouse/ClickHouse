#include <Coordination/Defines.h>
#include <Coordination/KeeperServer.h>

#include "config.h"

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/KeeperSnapshotManagerS3.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string.hpp>
#include <libnuraft/callback.hxx>
#include <libnuraft/cluster_config.hxx>
#include <libnuraft/log_val_type.hxx>
#include <libnuraft/msg_type.hxx>
#include <libnuraft/ptr.hxx>
#include <libnuraft/raft_server.hxx>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/Stopwatch.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#if USE_SSL
#    include <Server/CertificateReloader.h>
#    include <openssl/ssl.h>
#    include <Poco/Crypto/EVPPKey.h>
#    include <Poco/Net/Context.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/Utility.h>
#    include <Poco/StringTokenizer.h>
#endif

#include <chrono>
#include <mutex>
#include <string>

#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#include <fmt/chrono.h>

#include <libnuraft/req_msg.hxx>


namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

using namespace std::chrono_literals;

namespace
{

#if USE_SSL

int callSetCertificate(SSL * ssl, void * arg)
{
    if (!arg)
        return -1;

    const CertificateReloader::Data * data = reinterpret_cast<CertificateReloader::Data *>(arg);
    return setCertificateCallback(ssl, data, getLogger("SSLContext"));
}

void setSSLParams(nuraft::asio_service::options & asio_opts)
{
    const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
    String certificate_file_property = "openSSL.server.certificateFile";
    String private_key_file_property = "openSSL.server.privateKeyFile";
    String root_ca_file_property = "openSSL.server.caConfig";

    if (!config.has(certificate_file_property))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Server certificate file is not set.");

    if (!config.has(private_key_file_property))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Server private key file is not set.");

    Poco::Net::Context::Params params;
    params.certificateFile = config.getString(certificate_file_property);
    if (params.certificateFile.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Server certificate file in config '{}' is empty", certificate_file_property);

    params.privateKeyFile = config.getString(private_key_file_property);
    if (params.privateKeyFile.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Server key file in config '{}' is empty", private_key_file_property);

    auto pass_phrase = config.getString("openSSL.server.privateKeyPassphraseHandler.options.password", "");
    auto certificate_data = std::make_shared<CertificateReloader::Data>(params.certificateFile, params.privateKeyFile, pass_phrase);

    if (config.has(root_ca_file_property))
        params.caLocation = config.getString(root_ca_file_property);

    params.loadDefaultCAs = config.getBool("openSSL.server.loadDefaultCAFile", false);
    params.verificationMode = Poco::Net::Utility::convertVerificationMode(config.getString("openSSL.server.verificationMode", "none"));

    std::string disabled_protocols_list = config.getString("openSSL.server.disableProtocols", "");
    Poco::StringTokenizer dp_tok(disabled_protocols_list, ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
    int disabled_protocols = 0;
    for (const auto & token : dp_tok)
    {
        if (token == "sslv2")
            disabled_protocols |= Poco::Net::Context::PROTO_SSLV2;
        else if (token == "sslv3")
            disabled_protocols |= Poco::Net::Context::PROTO_SSLV3;
        else if (token == "tlsv1")
            disabled_protocols |= Poco::Net::Context::PROTO_TLSV1;
        else if (token == "tlsv1_1")
            disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_1;
        else if (token == "tlsv1_2")
            disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_2;
    }

    asio_opts.ssl_context_provider_server_ = [params, certificate_data, disabled_protocols]
    {
        Poco::Net::Context context(Poco::Net::Context::Usage::TLSV1_2_SERVER_USE, params);
        context.disableProtocols(disabled_protocols);
        SSL_CTX * ssl_ctx = context.takeSslContext();
        SSL_CTX_set_cert_cb(ssl_ctx, callSetCertificate, reinterpret_cast<void *>(certificate_data.get()));
        return ssl_ctx;
    };

    asio_opts.ssl_context_provider_client_ = [ctx_params = std::move(params)]
    {
        Poco::Net::Context context(Poco::Net::Context::Usage::TLSV1_2_CLIENT_USE, ctx_params);
        return context.takeSslContext();
    };
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

int32_t getValueOrMaxInt32AndLogWarning(uint64_t value, const std::string & name, LoggerPtr log)
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
    SnapshotsQueue & snapshots_queue_,
    KeeperContextPtr keeper_context_,
    KeeperSnapshotManagerS3 & snapshot_manager_s3,
    IKeeperStateMachine::CommitCallback commit_callback)
    : server_id(configuration_and_settings_->server_id)
    , log(getLogger("KeeperServer"))
    , is_recovering(config.getBool("keeper_server.force_recovery", false))
    , keeper_context{std::move(keeper_context_)}
    , create_snapshot_on_exit(config.getBool("keeper_server.create_snapshot_on_exit", true))
    , enable_reconfiguration(config.getBool("keeper_server.enable_reconfiguration", false))
{
    if (keeper_context->getCoordinationSettings()->quorum_reads)
        LOG_WARNING(log, "Quorum reads enabled, Keeper will work slower.");

#if USE_ROCKSDB
    const auto & coordination_settings = keeper_context->getCoordinationSettings();
    if (coordination_settings->experimental_use_rocksdb)
    {
        state_machine = nuraft::cs_new<KeeperStateMachine<KeeperRocksStorage>>(
            responses_queue_,
            snapshots_queue_,
            keeper_context,
            config.getBool("keeper_server.upload_snapshot_on_exit", false) ? &snapshot_manager_s3 : nullptr,
            commit_callback,
            checkAndGetSuperdigest(configuration_and_settings_->super_digest));
        LOG_WARNING(log, "Use RocksDB as Keeper backend storage.");
    }
    else
#endif
        state_machine = nuraft::cs_new<KeeperStateMachine<KeeperMemoryStorage>>(
            responses_queue_,
            snapshots_queue_,
            keeper_context,
            config.getBool("keeper_server.upload_snapshot_on_exit", false) ? &snapshot_manager_s3 : nullptr,
            commit_callback,
            checkAndGetSuperdigest(configuration_and_settings_->super_digest));

    state_manager = nuraft::cs_new<KeeperStateManager>(
        server_id,
        "keeper_server",
        "state",
        config,
        keeper_context);
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

    void commit_in_bg() override
    {
        // For NuRaft, if any commit fails (uncaught exception) the whole server aborts as a safety
        // This includes failed allocation which can produce an unknown state for the storage,
        // making it impossible to handle correctly.
        // We block the memory tracker for all the commit operations (including KeeperStateMachine::commit)
        // assuming that the allocations are small
        LockMemoryExceptionInThread blocker{VariableContext::Global};
        nuraft::raft_server::commit_in_bg();
    }

    std::unique_lock<std::recursive_mutex> lockRaft()
    {
        return std::unique_lock(lock_);
    }

    bool isCommitInProgress() const
    {
        return sm_commit_exec_in_progress_;
    }

    void setServingRequest(bool value)
    {
        serving_req_ = value;
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
    auto async_replication = keeper_context->getCoordinationSettings()->async_replication;

    if (latest_snapshot_config && latest_log_store_config)
    {
        if (latest_snapshot_config->get_log_idx() > latest_log_store_config->get_log_idx())
        {
            LOG_INFO(log, "Will use config from snapshot with log index {}", latest_snapshot_config->get_log_idx());
            latest_snapshot_config->set_async_replication(async_replication);
            state_manager->save_config(*latest_snapshot_config);
        }
        else
        {
            LOG_INFO(log, "Will use config from log store with log index {}", latest_log_store_config->get_log_idx());
            latest_log_store_config->set_async_replication(async_replication);
            state_manager->save_config(*latest_log_store_config);
        }
    }
    else if (latest_snapshot_config)
    {
        LOG_INFO(log, "No config in log store, will use config from snapshot with log index {}", latest_snapshot_config->get_log_idx());
        latest_snapshot_config->set_async_replication(async_replication);
        state_manager->save_config(*latest_snapshot_config);
    }
    else if (latest_log_store_config)
    {
        LOG_INFO(log, "No config in snapshot, will use config from log store with log index {}", latest_log_store_config->get_log_idx());
        latest_log_store_config->set_async_replication(async_replication);
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

void KeeperServer::launchRaftServer(const Poco::Util::AbstractConfiguration & config, bool enable_ipv6)
{
    const auto & coordination_settings = keeper_context->getCoordinationSettings();

    nuraft::raft_params params;
    params.parallel_log_appending_ = true;
    params.heart_beat_interval_
        = getValueOrMaxInt32AndLogWarning(coordination_settings->heart_beat_interval_ms.totalMilliseconds(), "heart_beat_interval_ms", log);
    params.election_timeout_lower_bound_ = getValueOrMaxInt32AndLogWarning(
        coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds(), "election_timeout_lower_bound_ms", log);
    params.election_timeout_upper_bound_ = getValueOrMaxInt32AndLogWarning(
        coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds(), "election_timeout_upper_bound_ms", log);

    if (params.election_timeout_lower_bound_ || params.election_timeout_upper_bound_)
    {
        if (params.election_timeout_lower_bound_ >= params.election_timeout_upper_bound_)
        {
            LOG_FATAL(
                log,
                "election_timeout_lower_bound_ms is greater than election_timeout_upper_bound_ms, this would disable leader election "
                "completely.");
            std::terminate();
        }
    }

    params.leadership_expiry_ = getValueOrMaxInt32AndLogWarning(
        coordination_settings->leadership_expiry_ms.totalMilliseconds(), "leadership_expiry_ms", log);

    if (params.leadership_expiry_ > 0 && params.leadership_expiry_ <= params.election_timeout_lower_bound_)
    {
        LOG_INFO(
            log,
            "leadership_expiry_ is smaller than or equal to election_timeout_lower_bound_ms, which can avoid multiple leaders. "
            "Notice that too small leadership_expiry_ may make Raft group sensitive to network status. "
            );
    }

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
    params.auto_forwarding_req_timeout_ = std::max<int32_t>(
        static_cast<int32_t>(coordination_settings->operation_timeout_ms.totalMilliseconds() * 2),
        std::numeric_limits<int32_t>::max());
    params.auto_forwarding_req_timeout_
        = getValueOrMaxInt32AndLogWarning(coordination_settings->operation_timeout_ms.totalMilliseconds() * 2, "operation_timeout_ms", log);
    params.max_append_size_
        = getValueOrMaxInt32AndLogWarning(coordination_settings->max_requests_append_size, "max_requests_append_size", log);

    params.return_method_ = nuraft::raft_params::async_handler;

    nuraft::asio_service::options asio_opts{};

    /// If asio worker threads fail in any way, NuRaft will stop to make any progress
    /// For that reason we need to suppress out of memory exceptions in such threads
    /// TODO: use `get_active_workers` to detect when we have no active workers to abort
    asio_opts.worker_start_ = [](uint32_t /*worker_id*/)
    {
        LockMemoryExceptionInThread::addUniqueLock(VariableContext::Global);
    };

    asio_opts.worker_stop_ = [](uint32_t /*worker_id*/)
    {
        LockMemoryExceptionInThread::removeUniqueLock();
    };

    /// At least 16 threads for network communication in asio.
    /// asio is async framework, so even with 1 thread it should be ok, but
    /// still as safeguard it's better to have some redundant capacity here
    asio_opts.thread_pool_size_ = std::max(16U, getNumberOfPhysicalCPUCores());

    if (state_manager->isSecure())
    {
#if USE_SSL
        setSSLParams(asio_opts);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for NuRaft is disabled because ClickHouse was built without SSL support.");
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

    // we use the same config as for the CH replicas because it is for internal communication between Keeper instances
    std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config, "", "interserver_listen_host");

    if (listen_hosts.empty())
    {
        auto asio_listener = asio_service->create_rpc_listener(state_manager->getPort(), logger, enable_ipv6);
        if (!asio_listener)
            throw Exception(ErrorCodes::RAFT_ERROR, "Cannot create interserver listener on port {}", state_manager->getPort());
        asio_listeners.emplace_back(std::move(asio_listener));
    }
    else
    {
        for (const auto & listen_host : listen_hosts)
        {
            auto asio_listener = asio_service->create_rpc_listener(listen_host, state_manager->getPort(), logger);
            if (asio_listener)
                asio_listeners.emplace_back(std::move(asio_listener));
        }
    }

    nuraft::ptr<nuraft::delayed_task_scheduler> scheduler = asio_service;
    nuraft::ptr<nuraft::rpc_client_factory> rpc_cli_factory = asio_service;

    nuraft::ptr<nuraft::state_mgr> casted_state_manager = state_manager;
    nuraft::ptr<nuraft::state_machine> casted_state_machine = state_machine;

    /// raft_server creates unique_ptr from it
    nuraft::context * ctx
        = new nuraft::context(casted_state_manager, casted_state_machine, asio_listeners, logger, rpc_cli_factory, scheduler, params);

    raft_instance = nuraft::cs_new<KeeperRaftServer>(ctx, init_options);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");

    state_manager->getLogStore()->setRaftServer(raft_instance);

    nuraft::raft_server::limits raft_limits;
    raft_limits.reconnect_limit_ = getValueOrMaxInt32AndLogWarning(coordination_settings->raft_limits_reconnect_limit, "raft_limits_reconnect_limit", log);
    raft_instance->set_raft_limits(raft_limits);

    raft_instance->start_server(init_options.skip_initial_election_timeout_);

    nuraft::ptr<nuraft::raft_server> casted_raft_server = raft_instance;

    for (const auto & asio_listener : asio_listeners)
    {
        asio_listener->listen(casted_raft_server);
    }
}

void KeeperServer::startup(const Poco::Util::AbstractConfiguration & config, bool enable_ipv6)
{
    state_machine->init();

    keeper_context->setLastCommitIndex(state_machine->last_commit_index());

    const auto & coordination_settings = keeper_context->getCoordinationSettings();

    state_manager->loadLogStore(state_machine->last_commit_index() + 1, coordination_settings->reserved_log_items);

    auto log_store = state_manager->load_log_store();
    last_log_idx_on_disk = log_store->next_slot() - 1;
    LOG_TRACE(log, "Last local log idx {}", last_log_idx_on_disk);
    if (state_machine->last_commit_index() >= last_log_idx_on_disk)
        keeper_context->setLocalLogsPreprocessed();

    loadLatestConfig();

    last_local_config = state_manager->parseServersConfiguration(config, true, coordination_settings->async_replication).cluster_config;

    launchRaftServer(config, enable_ipv6);

    keeper_context->setServerState(KeeperContext::Phase::RUNNING);
}

void KeeperServer::shutdownRaftServer()
{
    size_t timeout = keeper_context->getCoordinationSettings()->shutdown_timeout.totalSeconds();

    if (!raft_instance)
    {
        LOG_INFO(log, "RAFT doesn't start, shutdown not required");
        return;
    }

    raft_instance->shutdown();

    keeper_context->setServerState(KeeperContext::Phase::SHUTDOWN);

    if (create_snapshot_on_exit)
        raft_instance->create_snapshot();

    raft_instance.reset();

    for (const auto & asio_listener : asio_listeners)
    {
        if (asio_listener)
        {
            asio_listener->stop();
            asio_listener->shutdown();
        }
    }

    if (asio_service)
    {
        asio_service->stop();
        size_t count = 0;
        while (asio_service->get_active_workers() != 0 && count < timeout * 100)
        {
            std::this_thread::sleep_for(10ms);
            count++;
        }
    }

    if (asio_service->get_active_workers() != 0)
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", timeout);
}


void KeeperServer::shutdown()
{
    shutdownRaftServer();
    state_manager->flushAndShutDownLogStore();
    state_machine->shutdownStorage();
}

namespace
{

// Serialize the request for the log entry
nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(const KeeperStorageBase::RequestForSession & request_for_session)
{
    DB::WriteBufferFromNuraftBuffer write_buf;
    DB::writeIntBinary(request_for_session.session_id, write_buf);
    request_for_session.request->write(write_buf);
    DB::writeIntBinary(request_for_session.time, write_buf);
    /// we fill with dummy values to eliminate unnecessary copy later on when we will write correct values
    DB::writeIntBinary(static_cast<int64_t>(0), write_buf); /// zxid
    DB::writeIntBinary(KeeperStorageBase::DigestVersion::NO_DIGEST, write_buf); /// digest version or NO_DIGEST flag
    DB::writeIntBinary(static_cast<uint64_t>(0), write_buf); /// digest value
    /// if new fields are added, update KeeperStateMachine::ZooKeeperLogSerializationVersion along with parseRequest function and PreAppendLog callback handler
    return write_buf.getBuffer();
}

}

void KeeperServer::putLocalReadRequest(const KeeperStorageBase::RequestForSession & request_for_session)
{
    if (!request_for_session.request->isReadRequest())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot process non-read request locally");

    state_machine->processReadRequest(request_for_session);
}

RaftAppendResult KeeperServer::putRequestBatch(const KeeperStorageBase::RequestsForSessions & requests_for_sessions)
{
    std::vector<nuraft::ptr<nuraft::buffer>> entries;
    entries.reserve(requests_for_sessions.size());
    for (const auto & request_for_session : requests_for_sessions)
        entries.push_back(getZooKeeperLogEntry(request_for_session));

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
    return raft_instance && raft_instance->is_leader_alive();
}

bool KeeperServer::isExceedingMemorySoftLimit() const
{
    Int64 mem_soft_limit = keeper_context->getKeeperMemorySoftLimit();
    return mem_soft_limit > 0 && std::max(total_memory_tracker.get(), total_memory_tracker.getRSS()) >= mem_soft_limit;
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

    if (!keeper_context->localLogsPreprocessed())
    {
        const auto preprocess_logs = [&]
        {
            auto lock = raft_instance->lockRaft();

            if (keeper_context->localLogsPreprocessed())
                return;

            keeper_context->setLocalLogsPreprocessed();
            auto log_store = state_manager->load_log_store();
            auto log_entries = log_store->log_entries(state_machine->last_commit_index() + 1, log_store->next_slot());

            if (log_entries->empty())
            {
                LOG_INFO(log, "All local log entries preprocessed");
                return;
            }

            size_t preprocessed = 0;
            LOG_INFO(log, "Preprocessing {} log entries", log_entries->size());
            auto idx = state_machine->last_commit_index() + 1;
            for (const auto & entry : *log_entries)
            {
                if (entry && entry->get_val_type() == nuraft::log_val_type::app_log)
                    state_machine->pre_commit(idx, entry->get_buf());

                ++idx;
                ++preprocessed;

                if (preprocessed % 50000 == 0)
                    LOG_TRACE(log, "Preprocessed {}/{} entries", preprocessed, log_entries->size());
            }
            LOG_INFO(log, "Preprocessing done");
        };

        switch (type)
        {
            case nuraft::cb_func::PreAppendLogLeader:
            {
                /// we cannot preprocess anything new as leader because we don't have up-to-date in-memory state
                /// until we preprocess all stored logs
                return nuraft::cb_func::ReturnCode::ReturnNull;
            }
            case nuraft::cb_func::ProcessReq:
            {
                auto & req = *static_cast<nuraft::req_msg *>(param->ctx);

                if (req.get_type() != nuraft::msg_type::append_entries_request)
                    break;

                if (req.log_entries().empty())
                    break;

                /// committing/preprocessing of local logs can take some time
                /// and we don't want election to start during that time so we
                /// set serving requests to avoid elections on timeout
                raft_instance->setServingRequest(true);
                SCOPE_EXIT(raft_instance->setServingRequest(false));
                /// maybe we got snapshot installed
                if (state_machine->last_commit_index() >= last_log_idx_on_disk && !raft_instance->isCommitInProgress())
                    preprocess_logs();
                /// we don't want to append new logs if we are committing local logs
                else if (raft_instance->get_target_committed_log_idx() >= last_log_idx_on_disk)
                    keeper_context->waitLocalLogsPreprocessedOrShutdown();

                break;
            }
            case nuraft::cb_func::GotAppendEntryReqFromLeader:
            {
                auto & req = *static_cast<nuraft::req_msg *>(param->ctx);

                if (req.log_entries().empty())
                    break;

                if (req.get_last_log_idx() < last_log_idx_on_disk)
                    last_log_idx_on_disk = req.get_last_log_idx();

                break;
            }
            case nuraft::cb_func::StateMachineExecution:
            {
                if (state_machine->last_commit_index() >= last_log_idx_on_disk)
                    preprocess_logs();
                break;
            }
            default:
                break;
        }
    }

    const auto follower_preappend = [&](const auto & entry)
    {
        if (entry->get_val_type() != nuraft::app_log)
            return nuraft::cb_func::ReturnCode::Ok;

        try
        {
            state_machine->parseRequest(entry->get_buf(), /*final=*/false);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to parse request from log entry");
            throw;
        }
        return nuraft::cb_func::ReturnCode::Ok;

    };

    if (initialized_flag)
    {
        switch (type)
        {
            // This event is called before a single log is appended to the entry on the leader node
            case nuraft::cb_func::PreAppendLogLeader:
            {
                // we are relying on the fact that request are being processed under a mutex
                // and not a RW lock
                auto & entry = *static_cast<LogEntryPtr *>(param->ctx);

                assert(entry->get_val_type() == nuraft::app_log);
                auto next_zxid = state_machine->getNextZxid();

                auto entry_buf = entry->get_buf_ptr();

                IKeeperStateMachine::ZooKeeperLogSerializationVersion serialization_version;
                auto request_for_session = state_machine->parseRequest(*entry_buf, /*final=*/false, &serialization_version);
                request_for_session->zxid = next_zxid;
                if (!state_machine->preprocess(*request_for_session))
                    return nuraft::cb_func::ReturnCode::ReturnNull;

                request_for_session->digest = state_machine->getNodesDigest();

                /// older versions of Keeper can send logs that are missing some fields
                size_t bytes_missing = 0;
                if (serialization_version < IKeeperStateMachine::ZooKeeperLogSerializationVersion::WITH_TIME)
                    bytes_missing += sizeof(request_for_session->time);

                if (serialization_version < IKeeperStateMachine::ZooKeeperLogSerializationVersion::WITH_ZXID_DIGEST)
                    bytes_missing += sizeof(request_for_session->zxid) + sizeof(request_for_session->digest->version) + sizeof(request_for_session->digest->value);

                if (bytes_missing != 0)
                {
                    auto new_buffer = nuraft::buffer::alloc(entry_buf->size() + bytes_missing);
                    memcpy(new_buffer->data_begin(), entry_buf->data_begin(), entry_buf->size());
                    entry_buf = std::move(new_buffer);
                    entry = nuraft::cs_new<nuraft::log_entry>(entry->get_term(), entry_buf, entry->get_val_type());
                }

                size_t write_buffer_header_size
                    = sizeof(request_for_session->zxid) + sizeof(request_for_session->digest->version) + sizeof(request_for_session->digest->value);

                if (serialization_version < IKeeperStateMachine::ZooKeeperLogSerializationVersion::WITH_TIME)
                    write_buffer_header_size += sizeof(request_for_session->time);

                auto * buffer_start = reinterpret_cast<BufferBase::Position>(entry_buf->data_begin() + entry_buf->size() - write_buffer_header_size);

                WriteBufferFromPointer write_buf(buffer_start, write_buffer_header_size);

                if (serialization_version < IKeeperStateMachine::ZooKeeperLogSerializationVersion::WITH_TIME)
                    writeIntBinary(request_for_session->time, write_buf);

                writeIntBinary(request_for_session->zxid, write_buf);
                writeIntBinary(request_for_session->digest->version, write_buf);
                if (request_for_session->digest->version != KeeperStorageBase::NO_DIGEST)
                    writeIntBinary(request_for_session->digest->value, write_buf);

                write_buf.finalize();

                return nuraft::cb_func::ReturnCode::Ok;
            }
            case nuraft::cb_func::PreAppendLogFollower:
            {
                const auto & entry = *static_cast<LogEntryPtr *>(param->ctx);
                return follower_preappend(entry);
            }
            case nuraft::cb_func::AppendLogFailed:
            {
                // we are relying on the fact that request are being processed under a mutex
                // and not a RW lock
                auto & entry = *static_cast<LogEntryPtr *>(param->ctx);

                assert(entry->get_val_type() == nuraft::app_log);

                auto & entry_buf = entry->get_buf();
                auto request_for_session = state_machine->parseRequest(entry_buf, true);
                state_machine->rollbackRequest(*request_for_session, true);
                return nuraft::cb_func::ReturnCode::Ok;
            }
            default:
                return nuraft::cb_func::ReturnCode::Ok;
        }
    }

    size_t last_commited = state_machine->last_commit_index();
    size_t next_index = state_manager->getLogStore()->next_slot();
    bool commited_store = false;
    if (next_index < last_commited || next_index - last_commited <= 1)
        commited_store = true;

    auto set_initialized = [this]
    {
        {
            std::lock_guard lock(initialized_mutex);
            initialized_flag = true;
        }
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
                if (leader_index < our_index + keeper_context->getCoordinationSettings()->fresh_log_gap)
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
        case nuraft::cb_func::PreAppendLogLeader:
        {
            return nuraft::cb_func::ReturnCode::ReturnNull;
        }
        case nuraft::cb_func::PreAppendLogFollower:
        {
            const auto & entry = *static_cast<LogEntryPtr *>(param->ctx);
            return follower_preappend(entry);
        }
        default: /// ignore other events
            return nuraft::cb_func::ReturnCode::Ok;
    }
}

void KeeperServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);

    int64_t timeout = keeper_context->getCoordinationSettings()->startup_timeout.totalMilliseconds();
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        LOG_WARNING(log, "Failed to wait for RAFT initialization in {}ms, will continue in background", timeout);
}

std::vector<int64_t> KeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

KeeperServer::ConfigUpdateState KeeperServer::applyConfigUpdate(
    const ClusterUpdateAction & action, bool last_command_was_leader_change)
{
    using enum ConfigUpdateState;
    std::lock_guard _{server_write_mutex};

    if (const auto * add = std::get_if<AddRaftServer>(&action))
    {
        if (raft_instance->get_srv_config(add->id) != nullptr)
            return Accepted;

        auto resp = raft_instance->add_srv(static_cast<nuraft::srv_config>(*add));
        resp->get();
        return resp->get_accepted() ? Accepted : Declined;
    }
    else if (const auto * remove = std::get_if<RemoveRaftServer>(&action))
    {
        // This corner case is the most problematic. Issue follows: if we agree on a number
        // of commands but don't commit them on leader, and then issue a leadership change via
        // yield/request, leader can pause writes before all commits, therefore commands will be lost
        // (leadership change is not synchronized with committing in NuRaft).
        // However, waiting till some commands get _committed_ instead of _agreed_ is a hard task
        // regarding current library design, and this brings lots of levels of complexity
        // (see https://github.com/ClickHouse/ClickHouse/pull/53481 history). So, a compromise here
        // is a timeout before issuing a leadership change with an ability to change if user knows they
        // have a particularly slow network.
        if (remove->id == raft_instance->get_leader())
        {
            if (!last_command_was_leader_change)
                return WaitBeforeChangingLeader;

            if (isLeader())
                raft_instance->yield_leadership();
            else
                raft_instance->request_leadership();
            return Declined;
        }

        if (raft_instance->get_srv_config(remove->id) == nullptr)
            return Accepted;

        auto resp = raft_instance->remove_srv(remove->id);
        resp->get();
        return resp->get_accepted() ? Accepted : Declined;
    }
    else if (const auto * update = std::get_if<UpdateRaftServerPriority>(&action))
    {
        if (auto ptr = raft_instance->get_srv_config(update->id); ptr == nullptr)
            throw Exception(ErrorCodes::RAFT_ERROR,
                "Attempt to apply {} but server is not present in Raft",
                action);
        else if (ptr->get_priority() == update->priority)
            return Accepted;

        raft_instance->set_priority(update->id, update->priority, /*broadcast on live leader*/true);
        return Accepted;
    }
    std::unreachable();
}

ClusterUpdateActions KeeperServer::getRaftConfigurationDiff(const Poco::Util::AbstractConfiguration & config)
{
    const auto & coordination_settings = keeper_context->getCoordinationSettings();
    auto diff = state_manager->getRaftConfigurationDiff(config, coordination_settings);

    if (!diff.empty())
    {
        std::lock_guard lock{server_write_mutex};
        last_local_config = state_manager->parseServersConfiguration(config, true, coordination_settings->async_replication).cluster_config;
    }

    return diff;
}

void KeeperServer::applyConfigUpdateWithReconfigDisabled(const ClusterUpdateAction& action)
{
    std::lock_guard _{server_write_mutex};
    if (is_recovering) return;
    constexpr auto sleep_time = 500ms;

    LOG_INFO(log, "Will try to apply {}", action);

    auto applied = [&] { LOG_INFO(log, "Applied {}", action); };
    auto not_leader = [&] { LOG_INFO(log, "Not leader anymore, aborting"); };
    auto backoff_on_refusal = [&](size_t i)
    {
        LOG_INFO(log, "Update was not accepted (try {}), backing off for {}", i + 1, sleep_time * (i + 1));
        std::this_thread::sleep_for(sleep_time * (i + 1));
    };

    const auto & coordination_settings = keeper_context->getCoordinationSettings();
    if (const auto * add = std::get_if<AddRaftServer>(&action))
    {
        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(add->id) != nullptr)
                return applied(); // NOLINT
            if (!isLeader())
                return not_leader(); // NOLINT
            if (!raft_instance->add_srv(static_cast<nuraft::srv_config>(*add))->get_accepted())
                backoff_on_refusal(i);
        }
    }
    else if (const auto * remove = std::get_if<RemoveRaftServer>(&action))
    {
        if (remove->id == state_manager->server_id())
        {
            LOG_INFO(log,
                "Trying to remove leader node (ourself), so will yield leadership and some other node "
                "(new leader) will try to remove us. "
                "Probably you will have to run SYSTEM RELOAD CONFIG on the new leader node");
            raft_instance->yield_leadership();
            return;
        }

        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(remove->id) == nullptr)
                return applied(); // NOLINT
            if (!isLeader())
                return not_leader(); // NOLINT
            if (!raft_instance->remove_srv(remove->id)->get_accepted())
                backoff_on_refusal(i);
        }
    }
    else if (const auto * update = std::get_if<UpdateRaftServerPriority>(&action))
    {
        raft_instance->set_priority(update->id, update->priority, /*broadcast on live leader*/true);
        return;
    }

    throw Exception(ErrorCodes::RAFT_ERROR,
        "Configuration change {} was not accepted by Raft after {} retries",
        action, coordination_settings->configuration_change_tries_count);
}

bool KeeperServer::waitForConfigUpdateWithReconfigDisabled(const ClusterUpdateAction& action)
{
    if (is_recovering) return false;
    constexpr auto sleep_time = 500ms;

    LOG_INFO(log, "Will try to wait for {}", action);

    auto applied = [&] { LOG_INFO(log, "Applied {}", action); return true; };
    auto became_leader = [&] { LOG_INFO(log, "Became leader, aborting"); return false; };
    auto backoff = [&](size_t i) { std::this_thread::sleep_for(sleep_time * (i + 1)); };

    const auto & coordination_settings = keeper_context->getCoordinationSettings();
    if (const auto* add = std::get_if<AddRaftServer>(&action))
    {
        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(add->id) != nullptr)
                return applied();
            if (isLeader())
                return became_leader();
            backoff(i);
        }
    }
    else if (const auto* remove = std::get_if<RemoveRaftServer>(&action))
    {
        for (size_t i = 0; i < coordination_settings->configuration_change_tries_count && !is_recovering; ++i)
        {
            if (raft_instance->get_srv_config(remove->id) == nullptr)
                return applied();
            if (isLeader())
                return became_leader();
            backoff(i);
        }
    }
    else if (std::holds_alternative<UpdateRaftServerPriority>(action))
        return true;

    return false;
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
    result.is_exceeding_mem_soft_limit = isExceedingMemorySoftLimit();
    return result;
}

uint64_t KeeperServer::createSnapshot()
{
    uint64_t log_idx = raft_instance->create_snapshot();
    if (log_idx != 0)
        LOG_INFO(log, "Snapshot creation scheduled with last committed log index {}.", log_idx);
    else
        LOG_WARNING(log, "Failed to schedule snapshot creation task.");
    return log_idx;
}

KeeperLogInfo KeeperServer::getKeeperLogInfo()
{
    KeeperLogInfo log_info;
    auto log_store = state_manager->load_log_store();
    if (log_store)
    {
        const auto & keeper_log_storage = static_cast<const KeeperLogStore &>(*log_store);
        keeper_log_storage.getKeeperLogInfo(log_info);
    }

    if (raft_instance)
    {
        log_info.last_committed_log_idx = raft_instance->get_committed_log_idx();
        log_info.leader_committed_log_idx = raft_instance->get_leader_committed_log_idx();
        log_info.target_committed_log_idx = raft_instance->get_target_committed_log_idx();
        log_info.last_snapshot_idx = raft_instance->get_last_snapshot_idx();
    }

    return log_info;
}

bool KeeperServer::requestLeader()
{
    return isLeader() || raft_instance->request_leadership();
}

void KeeperServer::yieldLeadership()
{
    if (isLeader())
        raft_instance->yield_leadership();
}

void KeeperServer::recalculateStorageStats()
{
    state_machine->recalculateStorageStats();
}

}
