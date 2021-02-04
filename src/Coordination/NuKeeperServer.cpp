#include <Coordination/NuKeeperServer.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/InMemoryStateManager.h>
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
    extern const int TIMEOUT_EXCEEDED;
    extern const int RAFT_ERROR;
    extern const int LOGICAL_ERROR;
}

NuKeeperServer::NuKeeperServer(int server_id_, const std::string & hostname_, int port_)
    : server_id(server_id_)
    , hostname(hostname_)
    , port(port_)
    , endpoint(hostname + ":" + std::to_string(port))
    , state_machine(nuraft::cs_new<NuKeeperStateMachine>(500 /* FIXME */))
    , state_manager(nuraft::cs_new<InMemoryStateManager>(server_id, endpoint))
{
}

void NuKeeperServer::addServer(int server_id_, const std::string & server_uri_, bool can_become_leader_, int32_t priority)
{
    nuraft::srv_config config(server_id_, 0, server_uri_, "", /* learner = */ !can_become_leader_, priority);
    auto ret1 = raft_instance->add_srv(config);
    auto code = ret1->get_result_code();
    if (code == nuraft::cmd_result_code::TIMEOUT
        || code == nuraft::cmd_result_code::BAD_REQUEST
        || code == nuraft::cmd_result_code::NOT_LEADER
        || code == nuraft::cmd_result_code::FAILED)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot add server to RAFT quorum with code {}, message '{}'", ret1->get_result_code(), ret1->get_result_str());
}


void NuKeeperServer::startup()
{
    nuraft::raft_params params;
    params.heart_beat_interval_ = 500;
    params.election_timeout_lower_bound_ = 1000;
    params.election_timeout_upper_bound_ = 2000;
    params.reserved_log_items_ = 5000;
    params.snapshot_distance_ = 5000;
    params.client_req_timeout_ = 10000;
    params.auto_forwarding_ = true;
    params.return_method_ = nuraft::raft_params::blocking;

    nuraft::asio_service::options asio_opts{};

    raft_instance = launcher.init(
        state_machine, state_manager, nuraft::cs_new<LoggerWrapper>("RaftInstance"), port,
        asio_opts, params);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");

    static constexpr auto MAX_RETRY = 100;
    for (size_t i = 0; i < MAX_RETRY; ++i)
    {
        if (raft_instance->is_initialized())
            return;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot start RAFT server within startup timeout");
}

void NuKeeperServer::shutdown()
{
    state_machine->shutdownStorage();
    if (!launcher.shutdown(5))
        LOG_WARNING(&Poco::Logger::get("NuKeeperServer"), "Failed to shutdown RAFT server in {} seconds", 5);
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

NuKeeperStorage::ResponsesForSessions readZooKeeperResponses(nuraft::ptr<nuraft::buffer> & buffer, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::NuKeeperStorage::ResponsesForSessions results;
    DB::ReadBufferFromNuraftBuffer buf(buffer);
    bool response_found = false;

    while (!buf.eof())
    {
        int64_t session_id;
        DB::readIntBinary(session_id, buf);
        int32_t length;
        Coordination::XID xid;
        int64_t zxid;
        Coordination::Error err;

        Coordination::read(length, buf);
        Coordination::read(xid, buf);
        Coordination::read(zxid, buf);
        Coordination::read(err, buf);
        Coordination::ZooKeeperResponsePtr response;

        if (xid == Coordination::WATCH_XID)
            response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
        else
        {
            if (response_found)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "More than one non-watch response for single request with xid {}, response xid {}", request->xid, xid);

            response_found = true;
            response = request->makeResponse();
        }

        if (err == Coordination::Error::ZOK && (xid == Coordination::WATCH_XID || response->getOpNum() != Coordination::OpNum::Close))
            response->readImpl(buf);

        response->xid = xid;
        response->zxid = zxid;
        response->error = err;

        results.push_back(DB::NuKeeperStorage::ResponseForSession{session_id, response});
    }
    return results;
}

}

NuKeeperStorage::ResponsesForSessions NuKeeperServer::putRequest(const NuKeeperStorage::RequestForSession & request_for_session)
{
    auto [session_id, request] = request_for_session;
    if (isLeaderAlive() && request_for_session.request->isReadRequest())
    {
        return state_machine->processReadRequest(request_for_session);
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
            responses.push_back(DB::NuKeeperStorage::ResponseForSession{session_id, response});
            return responses;
        }

        if (result->get_result_code() == nuraft::cmd_result_code::TIMEOUT)
        {
            NuKeeperStorage::ResponsesForSessions responses;
            auto response = request->makeResponse();
            response->xid = request->xid;
            response->zxid = 0;
            response->error = Coordination::Error::ZOPERATIONTIMEOUT;
            responses.push_back(DB::NuKeeperStorage::ResponseForSession{session_id, response});
            return responses;
        }
        else if (result->get_result_code() != nuraft::cmd_result_code::OK)
            throw Exception(ErrorCodes::RAFT_ERROR, "Requests result failed with code {} and message: '{}'", result->get_result_code(), result->get_result_str());

        auto result_buf = result->get();
        if (result_buf == nullptr)
            throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr from RAFT leader");

        return readZooKeeperResponses(result_buf, request);
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

bool NuKeeperServer::waitForServer(int32_t id) const
{
    for (size_t i = 0; i < 50; ++i)
    {
        if (raft_instance->get_srv_config(id) != nullptr)
            return true;
        LOG_DEBUG(&Poco::Logger::get("NuRaftInit"), "Waiting for server {} to join the cluster", id);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

void NuKeeperServer::waitForServers(const std::vector<int32_t> & ids) const
{
    for (int32_t id : ids)
        waitForServer(id);
}

void NuKeeperServer::waitForCatchUp() const
{
    while (raft_instance->is_catching_up() || raft_instance->is_receiving_snapshot() || raft_instance->is_leader())
    {
        LOG_DEBUG(&Poco::Logger::get("NuRaftInit"), "Waiting current RAFT instance to catch up");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

std::unordered_set<int64_t> NuKeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

}
