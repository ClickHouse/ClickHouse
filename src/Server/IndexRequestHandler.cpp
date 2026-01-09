#include <Server/IndexRequestHandler.h>
#include <Server/StaticRequestHandler.h>
#include <Server/HTTPResponseHeaderWriter.h>
#include <IO/HTTPCommon.h>

#include <incbin.h>

INCBIN(resource_index_html, SOURCE_DIR "/programs/server/index.html");


namespace DB
{

IndexRequestHandler::IndexRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
    : server(server_), http_response_headers_override(http_response_headers_override_)
{
}

void IndexRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponseBase & response)
{
    /// If it looks like a browser:
    if (request.get("User-Agent", "").starts_with("Mozilla"))
    {
        applyHTTPResponseHeaders(response, http_response_headers_override);
        response.setContentType("text/html; charset=UTF-8");
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        response.setResponseDefaultHeaders();
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        auto wb = response.makeStream();
        wb->write(reinterpret_cast<const char *>(gresource_index_htmlData), gresource_index_htmlSize);
        wb->finalize();
    }
    else
    {
        StaticRequestHandler(server, "Ok.\n", parseHTTPResponseHeaders("text/html; charset=UTF-8")).handleRequest(request, response);
    }
}

}
