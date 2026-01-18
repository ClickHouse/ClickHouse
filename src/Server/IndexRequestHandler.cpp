#include <Server/IndexRequestHandler.h>
#include <Server/StaticRequestHandler.h>
#include <Server/HTTPResponseHeaderWriter.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/HTTPCommon.h>

constexpr unsigned char resource_index_html[] =
{
#embed "../../programs/server/index.html"
};


namespace DB
{

IndexRequestHandler::IndexRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
    : server(server_), http_response_headers_override(http_response_headers_override_)
{
}

void IndexRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event)
{
    /// If it looks like a browser:
    if (request.get("User-Agent", "").starts_with("Mozilla"))
    {
        applyHTTPResponseHeaders(response, http_response_headers_override);
        response.setContentType("text/html; charset=UTF-8");
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
        auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
        wb.write(reinterpret_cast<const char *>(resource_index_html), std::size(resource_index_html));
        wb.finalize();
    }
    else
    {
        StaticRequestHandler(server, "Ok.\n", parseHTTPResponseHeaders("text/html; charset=UTF-8")).handleRequest(request, response, write_event);
    }
}

}
