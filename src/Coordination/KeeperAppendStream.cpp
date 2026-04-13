#include <Coordination/KeeperAppendStream.h>

#if USE_NURAFT

#include <Coordination/KeeperServer.h>
#include <Coordination/CoordinationSettings.h>

#include <libnuraft/log_val_type.hxx>
#include <libnuraft/msg_type.hxx>
#include <libnuraft/req_msg.hxx>

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsMilliseconds operation_timeout_ms;
}

KeeperAppendStream::KeeperAppendStream(KeeperServer * server_) : server(server_) {}

bool KeeperAppendStream::isBroken() const
{
    /// Note: this needs to be cheap. get_term() just reads an atomic.
    return is_broken->load() || (term != 0 && server->raft_instance->get_term() != term);
}

void KeeperAppendStream::markAsBroken()
{
    is_broken->store(true);
}

void KeeperAppendStream::putRequestBatch(const KeeperRequestsForSessions & requests_for_sessions, std::function<void(bool)> callback)
{
    if (is_broken->load())
    {
        if (callback)
            callback(false);
        return;
    }

    auto fail = [&]
    {
        is_broken->store(true);
        if (callback)
            callback(false);
    };

    if (term == 0)
    {
        int32_t leader_id = server->raft_instance->get_leader();
        term = server->raft_instance->get_term();
        if (leader_id == -1 || term == 0)
        {
            fail();
            return;
        }
        int32_t my_id = server->raft_instance->get_id();

        if (leader_id != my_id)
        {
            auto c_conf = server->raft_instance->get_config();
            auto srv_conf = c_conf->get_server(leader_id);
            client = server->asio_service->create_client(srv_conf->get_endpoint());
            if (!client)
            {
                fail();
                return;
            }
        }
    }

    std::vector<nuraft::ptr<nuraft::buffer>> entries;
    entries.reserve(requests_for_sessions.size());
    for (const auto & request_for_session : requests_for_sessions)
        entries.push_back(IKeeperStateMachine::getZooKeeperLogEntry(request_for_session));

    if (client)
    {
        /// Form the message the same way as nuraft's append_entries.
        /// See comment on STREAM_FORWARDING in contrib/NuRaft/src/asio_service.cxx for explanation.
        auto req = nuraft::cs_new<nuraft::req_msg>(term, nuraft::msg_type::client_request, 0, 0, 0, 0, 0);
        req->set_extra_flags(nuraft::req_msg::STREAM_FORWARDING_REQUEST);
        req->log_entries().reserve(entries.size());
        for (auto & buf : entries)
        {
            buf->pos(0);
            auto log = nuraft::cs_new<nuraft::log_entry>(0, buf, nuraft::log_val_type::app_log);
            req->log_entries().push_back(log);
        }

        uint64_t timeout_ms = server->keeper_context->getCoordinationSettings()[CoordinationSetting::operation_timeout_ms].totalMilliseconds();
        std::function<void(nuraft::ptr<nuraft::resp_msg> &, nuraft::ptr<nuraft::rpc_exception> & err)> wrapped_callback =
            [is_broken_ = is_broken, callback](nuraft::ptr<nuraft::resp_msg> &, nuraft::ptr<nuraft::rpc_exception> & err)
            {
                if (err != nullptr)
                    is_broken_->store(true);
                if (callback)
                    callback(err == nullptr);
            };
        client->send(req, wrapped_callback, timeout_ms);
    }
    else
    {
        nuraft::raft_server::req_ext_params params;
        params.expected_term_ = term;
        auto res = server->raft_instance->append_entries_ext(entries, params);
        if (!res || !res->get_accepted())
        {
            fail();
            return;
        }

        /// We pretend that this is async, but actually `res` is always ready here and doesn't need
        /// a callback, when async replication is enabled.
        res->when_ready(
            [is_broken_ = is_broken, callback](nuraft::ptr<nuraft::buffer> &, nuraft::ptr<std::exception> & err)
            {
                if (err != nullptr)
                    is_broken_->store(true);
                if (callback)
                    callback(err == nullptr);
            });
    }
}

}

#endif
