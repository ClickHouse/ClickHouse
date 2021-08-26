#include <Coordination/NuKeeperServer.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/NuKeeperStateManager.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <chrono>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

NuKeeperServer::NuKeeperServer(
    int server_id_,
    const CoordinationSettingsPtr & coordination_settings_,
    const Poco::Util::AbstractConfiguration & config,
    ResponsesQueue & responses_queue_)
    : server_id(server_id_)
    , coordination_settings(coordination_settings_)
    , state_machine(nuraft::cs_new<NuKeeperStateMachine>(responses_queue_, coordination_settings))
    , state_manager(nuraft::cs_new<NuKeeperStateManager>(server_id, "test_keeper_server", config, coordination_settings))
    , responses_queue(responses_queue_)
{
}

void NuKeeperServer::startup()
{

    state_manager->loadLogStore(state_machine->last_commit_index());
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
    params.client_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds();
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    params.auto_forwarding_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds() * 2;

    params.return_method_ = nuraft::raft_params::blocking;

    nuraft::asio_service::options asio_opts{};
    nuraft::raft_server::init_options init_options;
    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.raft_callback_ = [this] (nuraft::cb_func::Type type, nuraft::cb_func::Param * param)
    {
        return callbackFunc(type, param);
    };

    raft_instance = launcher.init(
        state_machine, state_manager, nuraft::cs_new<LoggerWrapper>("RaftInstance", coordination_settings->raft_logs_level), state_manager->getPort(),
        asio_opts, params, init_options);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

void NuKeeperServer::shutdown()
{
    state_machine->shutdownStorage();
    state_manager->flushLogStore();
    auto timeout = coordination_settings->shutdown_timeout.totalSeconds();
    if (!launcher.shutdown(timeout))
        LOG_WARNING(&Poco::Logger::get("NuKeeperServer"), "Failed to shutdown RAFT server in {} seconds", timeout);
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

void NuKeeperServer::putRequest(const NuKeeperStorage::RequestForSession & request_for_session)
{
    auto [session_id, request] = request_for_session;
    if (isLeaderAlive() && request->isReadRequest())
    {
        state_machine->processReadRequest(request_for_session);
    }
    else
    {
        std::vector<nuraft::ptr<nuraft::buffer>> entries;
        entries.push_back(getZooKeeperLogEntry(session_id, request));

        std::lock_guard lock(append_entries_mutex);

        auto result = raft_instance->append_entries(entries);
        if (!result->get_accepted())
        {
            NuKeeperStorage::ResponsesForSessions responses;
            auto response = request->makeResponse();
            response->xid = request->xid;
            response->zxid = 0;
            response->error = Coordination::Error::ZOPERATIONTIMEOUT;
            responses_queue.push(DB::NuKeeperStorage::ResponseForSession{session_id, response});
        }

        if (result->get_result_code() == nuraft::cmd_result_code::TIMEOUT)
        {
            NuKeeperStorage::ResponsesForSessions responses;
            auto response = request->makeResponse();
            response->xid = request->xid;
            response->zxid = 0;
            response->error = Coordination::Error::ZOPERATIONTIMEOUT;
            responses_queue.push(DB::NuKeeperStorage::ResponseForSession{session_id, response});
        }
        else if (result->get_result_code() != nuraft::cmd_result_code::OK)
            throw Exception(ErrorCodes::RAFT_ERROR, "Requests result failed with code {} and message: '{}'", result->get_result_code(), result->get_result_str());
    }
}

int64_t NuKeeperServer::getSessionID(int64_t session_timeout_ms)
{
    auto entry = nuraft::buffer::alloc(sizeof(int64_t));
    /// Just special session request
    nuraft::buffer_serializer bs(entry);
    bs.put_i64(session_timeout_ms);

    std::lock_guard lock(append_entries_mutex);

    auto result = raft_instance->append_entries({entry});

    if (!result->get_accepted())
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot send session_id request to RAFT");

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        throw Exception(ErrorCodes::RAFT_ERROR, "session_id request failed to RAFT");

    auto resp = result->get();
    if (resp == nullptr)
        throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr as session_id");

    nuraft::buffer_serializer bs_resp(resp);
    return bs_resp.get_i64();
}

bool NuKeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}

bool NuKeeperServer::isLeaderAlive() const
{
    return raft_instance->is_leader_alive();
}

nuraft::cb_func::ReturnCode NuKeeperServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * /* param */)
{
    /// Only initial record
    bool empty_store = state_manager->getLogStore()->size() == 1;

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
            if (empty_store) /// We become leader and store is empty, ready to serve requests
                set_initialized();
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::BecomeFresh:
        {
            set_initialized(); /// We are fresh follower, ready to serve requests.
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::InitialBatchCommited:
        {
            if (isLeader()) /// We have committed our log store and we are leader, ready to serve requests.
                set_initialized();
            return nuraft::cb_func::ReturnCode::Ok;
        }
        default: /// ignore other events
            return nuraft::cb_func::ReturnCode::Ok;
    }
}

void NuKeeperServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = coordination_settings->startup_timeout.totalMilliseconds();
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag; }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::unordered_set<int64_t> NuKeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

}
