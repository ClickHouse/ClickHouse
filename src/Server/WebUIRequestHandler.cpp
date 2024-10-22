#include "WebUIRequestHandler.h"
#include "IServer.h"
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Core/ServerSettings.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Common/re2.h>

#include <incbin.h>

#include "config.h"

/// Embedded HTML pages
INCBIN(resource_play_html, SOURCE_DIR "/programs/server/play.html");
INCBIN(resource_dashboard_html, SOURCE_DIR "/programs/server/dashboard.html");
INCBIN(resource_uplot_js, SOURCE_DIR "/programs/server/js/uplot.js");
INCBIN(resource_lz_string_js, SOURCE_DIR "/programs/server/js/lz-string.js");
INCBIN(resource_binary_html, SOURCE_DIR "/programs/server/binary.html");


namespace DB
{

PlayWebUIRequestHandler::PlayWebUIRequestHandler(IServer & server_) : server(server_) {}
DashboardWebUIRequestHandler::DashboardWebUIRequestHandler(IServer & server_) : server(server_) {}
BinaryWebUIRequestHandler::BinaryWebUIRequestHandler(IServer & server_) : server(server_) {}
JavaScriptWebUIRequestHandler::JavaScriptWebUIRequestHandler(IServer & server_) : server(server_) {}

static void handle(HTTPServerRequest & request, HTTPServerResponse & response, std::string_view html)
{
    response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD).write(html.data(), html.size());
}

void PlayWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(gresource_play_htmlData), gresource_play_htmlSize});
}

void DashboardWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    std::string html(reinterpret_cast<const char *>(gresource_dashboard_htmlData), gresource_dashboard_htmlSize);

    /// Replace a link to external JavaScript file to embedded file.
    /// This allows to open the HTML without running a server and to host it on server.
    /// Note: we can embed the JavaScript file inline to the HTML,
    /// but we don't do it to keep the "view-source" perfectly readable.

    static re2::RE2 uplot_url = R"(https://[^\s"'`]+u[Pp]lot[^\s"'`]*\.js)";
    RE2::Replace(&html, uplot_url, "/js/uplot.js");

    static re2::RE2 lz_string_url = R"(https://[^\s"'`]+lz-string[^\s"'`]*\.js)";
    RE2::Replace(&html, lz_string_url, "/js/lz-string.js");

    handle(request, response, html);
}

void BinaryWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(gresource_binary_htmlData), gresource_binary_htmlSize});
}

void JavaScriptWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    if (request.getURI() == "/js/uplot.js")
    {
        handle(request, response, {reinterpret_cast<const char *>(gresource_uplot_jsData), gresource_uplot_jsSize});
    }
    else if (request.getURI() == "/js/lz-string.js")
    {
        handle(request, response, {reinterpret_cast<const char *>(gresource_lz_string_jsData), gresource_lz_string_jsSize});
    }
    else
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "Not found.\n";
    }

    handle(request, response, {reinterpret_cast<const char *>(gresource_binary_htmlData), gresource_binary_htmlSize});
}

}
