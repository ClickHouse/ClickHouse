#include "WebUIRequestHandler.h"
#include "IServer.h"
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Interpreters/Context.h>
#include <IO/HTTPCommon.h>
#include <Common/re2.h>

#include <incbin.h>

#include "config.h"

/// Embedded HTML pages
INCBIN(resource_play_html, SOURCE_DIR "/programs/server/play.html");
INCBIN(resource_dashboard_html, SOURCE_DIR "/programs/server/dashboard.html");
INCBIN(resource_uplot_js, SOURCE_DIR "/programs/server/js/uplot.js");
INCBIN(resource_binary_html, SOURCE_DIR "/programs/server/binary.html");


namespace DB
{

WebUIRequestHandler::WebUIRequestHandler(IServer & server_)
    : server(server_)
{
}


void WebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    auto keep_alive_timeout = server.context()->getServerSettings().keep_alive_timeout.totalSeconds();

    response.setContentType("text/html; charset=UTF-8");

    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response, keep_alive_timeout);

    if (request.getURI().starts_with("/play"))
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD, keep_alive_timeout).write(reinterpret_cast<const char *>(gresource_play_htmlData), gresource_play_htmlSize);
    }
    else if (request.getURI().starts_with("/dashboard"))
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);

        std::string html(reinterpret_cast<const char *>(gresource_dashboard_htmlData), gresource_dashboard_htmlSize);

        /// Replace a link to external JavaScript file to embedded file.
        /// This allows to open the HTML without running a server and to host it on server.
        /// Note: we can embed the JavaScript file inline to the HTML,
        /// but we don't do it to keep the "view-source" perfectly readable.

        static re2::RE2 uplot_url = R"(https://[^\s"'`]+u[Pp]lot[^\s"'`]*\.js)";
        RE2::Replace(&html, uplot_url, "/js/uplot.js");

        WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD, keep_alive_timeout).write(html);
    }
    else if (request.getURI().starts_with("/binary"))
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD, keep_alive_timeout).write(reinterpret_cast<const char *>(gresource_binary_htmlData), gresource_binary_htmlSize);
    }
    else if (request.getURI() == "/js/uplot.js")
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD, keep_alive_timeout).write(reinterpret_cast<const char *>(gresource_uplot_jsData), gresource_uplot_jsSize);
    }
    else
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "Not found.\n";
    }
}

}
