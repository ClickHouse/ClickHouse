#include <Server/KeeperDashboardRequestHandler.h>

#if USE_NURAFT

#include <Coordination/KeeperStorage.h>
#include <Server/HTTP/HTTPResponseHelpers.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Common/config_version.h>
#include <Common/re2.h>

/// Embedded HTML pages
constexpr unsigned char resource_keeper_dashboard_html[] =
{
#embed "../../programs/keeper/dashboard.html"
};

namespace DB
{

void KeeperDashboardWebUIRequestHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string_view html(reinterpret_cast<const char *>(resource_keeper_dashboard_html), std::size(resource_keeper_dashboard_html));

    response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto buf = responseWriteBuffer(request, response);
    buf.get()->write(html.data(), html.size());
    buf.get()->finalize();
}

void KeeperDashboardContentRequestHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
try
{
    Poco::JSON::Object response_json;

    response_json.set("ch_version", VERSION_DESCRIBE);

    if (keeper_dispatcher->isServerActive())
    {
        Poco::JSON::Object keeper_details;
        auto & stats = keeper_dispatcher->getKeeperConnectionStats();
        Keeper4LWInfo keeper_info = keeper_dispatcher->getKeeper4LWInfo();
        const auto storage_stats = keeper_dispatcher->getStateMachine().getStorageStats();

        keeper_details.set("latency_avg", stats.getAvgLatency());
        keeper_details.set("role", keeper_info.getRole());
        keeper_details.set("alive_connections", toString(keeper_info.alive_connections_count));
        keeper_details.set("node_count", toString(storage_stats.nodes_count));

        response_json.set("keeper_details", keeper_details);

        Poco::JSON::Object cluster;
        Poco::JSON::Array members;
        Int32 leader_id = 0;
        Int32 self_id = 0;
        bool peer_health_available = false;

        for (const auto & m : keeper_dispatcher->getClusterMembersInfo())
        {
            if (m.is_leader)
                leader_id = m.server_id;
            if (m.is_self)
                self_id = m.server_id;
            if (m.is_alive.has_value())
                peer_health_available = true;

            Poco::JSON::Object member;
            member.set("server_id", m.server_id);
            member.set("endpoint", m.endpoint);
            member.set("is_observer", m.is_observer);
            member.set("priority", m.priority);
            member.set("is_leader", m.is_leader);
            member.set("is_self", m.is_self);
            /// Self reports its own log index; peers are reported from the leader's view.
            /// Serialized as a string: UInt64 does not fit into a JSON number without
            /// precision loss above 2^53, and the UI does the lag arithmetic with BigInt.
            const auto & last_log_index = m.last_log_index.has_value() ? m.last_log_index : m.peer_last_log_index;
            if (last_log_index.has_value())
                member.set("last_log_index", toString(*last_log_index));
            else
                member.set("last_log_index", Poco::Dynamic::Var());
            if (m.is_alive.has_value())
                member.set("is_alive", *m.is_alive);
            else
                member.set("is_alive", Poco::Dynamic::Var());
            if (m.is_synced.has_value())
                member.set("is_synced", *m.is_synced);
            else
                member.set("is_synced", Poco::Dynamic::Var());
            if (m.last_succ_resp_ms.has_value())
                member.set("last_succ_resp_ms", *m.last_succ_resp_ms);
            else
                member.set("last_succ_resp_ms", Poco::Dynamic::Var());
            members.add(member);
        }

        cluster.set("leader_id", leader_id);
        cluster.set("self_id", self_id);
        cluster.set("has_leader", keeper_info.has_leader);
        cluster.set("peer_health_available", peer_health_available);
        /// HTTP control port of this node (from the accepted socket), used by the UI to deep-link peers.
        cluster.set("http_port", static_cast<unsigned>(request.serverAddress().port()));
        cluster.set("members", members);
        response_json.set("cluster", cluster);
    }

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(response_json, oss);

    response.setContentType("application/json");
    *response.send() << oss.str();
}
catch (...)
{
    tryLogCurrentException("KeeperDashboardContentRequestHandler");

    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        if (!response.sent())
        {
            /// We have not sent anything yet and we don't even know if we need to compress response.
            *response.send() << getCurrentExceptionMessage(false) << '\n';
        }
    }
    catch (...)
    {
        LOG_ERROR(getLogger("KeeperDashboardContentRequestHandler"), "Cannot send exception to client");
    }
}

}
#endif
