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


NuKeeperServer::NuKeeperServer(int server_id_, const std::string & hostname_, int port_)
    : server_id(server_id_)
    , hostname(hostname_)
    , port(port_)
    , endpoint(hostname + ":" + std::to_string(port))
    , state_machine(nuraft::cs_new<NuKeeperStateMachine>())
    , state_manager(nuraft::cs_new<InMemoryStateManager>(server_id, endpoint))
{
}

NuraftError NuKeeperServer::addServer(int server_id_, const std::string & server_uri_)
{
    nuraft::srv_config config(server_id_, server_uri_);
    auto ret1 = raft_instance->add_srv(config);
    return NuraftError{ret1->get_result_code(), ret1->get_result_str()};
}


NuraftError NuKeeperServer::startup()
{
    nuraft::raft_params params;
    params.heart_beat_interval_ = 100;
    params.election_timeout_lower_bound_ = 200;
    params.election_timeout_upper_bound_ = 400;
    params.reserved_log_items_ = 5;
    params.snapshot_distance_ = 5;
    params.client_req_timeout_ = 3000;
    params.return_method_ = nuraft::raft_params::blocking;

    raft_instance = launcher.init(
        state_machine, state_manager, nuraft::cs_new<LoggerWrapper>("RaftInstance"), port,
        nuraft::asio_service::options{}, params);

    if (!raft_instance)
        return NuraftError{nuraft::cmd_result_code::TIMEOUT, "Cannot create RAFT instance"};

    static constexpr auto MAX_RETRY = 30;
    for (size_t i = 0; i < MAX_RETRY; ++i)
    {
        if (raft_instance->is_initialized())
            return NuraftError{nuraft::cmd_result_code::OK, ""};

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return NuraftError{nuraft::cmd_result_code::TIMEOUT, "Cannot start RAFT instance"};
}

NuraftError NuKeeperServer::shutdown()
{
    if (!launcher.shutdown(5))
        return NuraftError{nuraft::cmd_result_code::TIMEOUT, "Temout waiting RAFT instance to shutdown"};
    return NuraftError{nuraft::cmd_result_code::OK, ""};
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

TestKeeperStorage::ResponsesForSessions NuKeeperServer::readZooKeeperResponses(nuraft::ptr<nuraft::buffer> & buffer)
{
    DB::TestKeeperStorage::ResponsesForSessions results;
    DB::ReadBufferFromNuraftBuffer buf(buffer);

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
            response = ops_mapping[session_id][xid];
            ops_mapping[session_id].erase(xid);
            if (ops_mapping[session_id].empty())
                ops_mapping.erase(session_id);
        }

        if (err == Coordination::Error::ZOK && (xid == Coordination::WATCH_XID || response->getOpNum() != Coordination::OpNum::Close))
            response->readImpl(buf);

        response->xid = xid;
        response->zxid = zxid;
        response->error = err;

        results.push_back(DB::TestKeeperStorage::ResponseForSession{session_id, response});
    }
    return results;
}

TestKeeperStorage::ResponsesForSessions NuKeeperServer::putRequests(const TestKeeperStorage::RequestsForSessions & requests)
{
    std::vector<nuraft::ptr<nuraft::buffer>> entries;
    for (auto & [session_id, request] : requests)
    {
        ops_mapping[session_id][request->xid] = request->makeResponse();
        entries.push_back(getZooKeeperLogEntry(session_id, request));
    }

    auto result = raft_instance->append_entries(entries);
    if (!result->get_accepted())
        return {};

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        return {};

    return readZooKeeperResponses(result->get());
}


int64_t NuKeeperServer::getSessionID()
{
    auto entry = nuraft::buffer::alloc(sizeof(size_t));
    nuraft::buffer_serializer bs(entry);
    bs.put_i64(0);

    auto result = raft_instance->append_entries({entry});
    if (!result->get_accepted())
        return -1;

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        return -1;

    auto resp = result->get();
    nuraft::buffer_serializer bs_resp(resp);
    return bs_resp.get_i64();
}

}
