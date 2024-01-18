#include "WebUIRequestHandler.h"
#include "IServer.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <IO/HTTPCommon.h>
#include <Common/getResource.h>

#include <re2/re2.h>


namespace DB
{

WebUIRequestHandler::WebUIRequestHandler(IServer & server_)
    : server(server_)
{
}


void WebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", 10);

    response.setContentType("text/html; charset=UTF-8");

    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response, keep_alive_timeout);

    if (request.getURI().starts_with("/play"))
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        *response.send() << getResource("play.html");
    }
    else if (request.getURI().starts_with("/dashboard"))
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);

        std::string html(getResource("dashboard.html"));

        /// Replace a link to external JavaScript file to embedded file.
        /// This allows to open the HTML without running a server and to host it on server.
        /// Note: we can embed the JavaScript file inline to the HTML,
        /// but we don't do it to keep the "view-source" perfectly readable.

        static re2::RE2 uplot_url = R"(https://[^\s"'`]+u[Pp]lot[^\s"'`]*\.js)";
        RE2::Replace(&html, uplot_url, "/js/uplot.js");

        *response.send() << html;
    }
    else if (request.getURI() == "/js/uplot.js")
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        *response.send() << getResource("js/uplot.js");
    }
    else
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        *response.send() << "Not found.\n";
    }
}

}
