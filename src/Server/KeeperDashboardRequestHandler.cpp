#include <Server/KeeperDashboardRequestHandler.h>

#if USE_NURAFT

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

#include <incbin.h>

/// Embedded HTML pages
INCBIN(resource_keeper_dashboard_html, SOURCE_DIR "/programs/keeper/dashboard.html");


namespace DB
{

void KeeperDashboardWebUIRequestHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string html(reinterpret_cast<const char *>(gresource_keeper_dashboard_htmlData), gresource_keeper_dashboard_htmlSize);

    /// Replace a link to external JavaScript file to embedded file.
    /// This allows to open the HTML without running a server and to host it on server.
    /// Note: we can embed the JavaScript file inline to the HTML,
    /// but we don't do it to keep the "view-source" perfectly readable.

    static re2::RE2 uplot_url = R"(https://[^\s"'`]+u[Pp]lot[^\s"'`]*\.js)";
    RE2::Replace(&html, uplot_url, "/js/uplot.js");

    static re2::RE2 lz_string_url = R"(https://[^\s"'`]+lz-string[^\s"'`]*\.js)";
    RE2::Replace(&html, lz_string_url, "/js/lz-string.js");

    response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD)
        .write(html.data(), html.size());
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

        keeper_details.set("latency_avg", stats.getAvgLatency());
        keeper_details.set("role", keeper_info.getRole());
        keeper_details.set("alive_connections", toString(keeper_info.alive_connections_count));
        keeper_details.set("node_count", toString(keeper_info.total_nodes_count));

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
