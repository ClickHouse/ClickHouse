#include <Server/KeeperDashboardRequestHandler.h>

#if USE_NURAFT

#include <Coordination/KeeperStorage.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>
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
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(html.data(), html.size());
    wb.finalize();
}

void KeeperDashboardContentRequestHandler::handleRequest(
    HTTPServerRequest & /*request*/, HTTPServerResponse & response, const ProfileEvents::Event &)
try
{
    Poco::JSON::Object response_json;

    response_json.set("ch_version", VERSION_DESCRIBE);

    if (keeper_dispatcher->isServerActive())
    {
        Poco::JSON::Object keeper_details;
        auto & stats = keeper_dispatcher->getKeeperConnectionStats();
        Keeper4LWInfo keeper_info = keeper_dispatcher->getKeeper4LWInfo();
        const auto & storage_stats = keeper_dispatcher->getStateMachine().getStorageStats();

        keeper_details.set("latency_avg", stats.getAvgLatency());
        keeper_details.set("role", keeper_info.getRole());
        keeper_details.set("alive_connections", toString(keeper_info.alive_connections_count));
        keeper_details.set("node_count", toString(storage_stats.nodes_count.load(std::memory_order_relaxed)));

        response_json.set("keeper_details", keeper_details);
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
