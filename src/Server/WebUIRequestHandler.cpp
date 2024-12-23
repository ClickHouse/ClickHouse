#include <Server/WebUIRequestHandler.h>

#include <incbin.h>

#include <Common/re2.h>
#include <Core/ServerSettings.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#if USE_SSL
#include <Server/ACMEClient.h>
#endif


/// Embedded HTML pages
INCBIN(resource_play_html, SOURCE_DIR "/programs/server/play.html");
INCBIN(resource_dashboard_html, SOURCE_DIR "/programs/server/dashboard.html");
INCBIN(resource_uplot_js, SOURCE_DIR "/programs/server/js/uplot.js");
INCBIN(resource_lz_string_js, SOURCE_DIR "/programs/server/js/lz-string.js");
INCBIN(resource_binary_html, SOURCE_DIR "/programs/server/binary.html");
INCBIN(resource_merges_html, SOURCE_DIR "/programs/server/merges.html");


namespace DB
{

static void handle(HTTPServerRequest & request, HTTPServerResponse & response, std::string_view html)
{
    response.setContentType("text/html; charset=UTF-8");
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
    wb.write(html.data(), html.size());
    wb.finalize();
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

void MergesWebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    handle(request, response, {reinterpret_cast<const char *>(gresource_merges_htmlData), gresource_merges_htmlSize});
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

#if USE_SSL
void ACMERequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    auto challenge = ACMEClient::ACMEClient::instance().requestChallenge(request.getURI());

    if (challenge.empty())
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "Not found.\n";
    }

    handle(request, response, { challenge });
}
#endif


}
