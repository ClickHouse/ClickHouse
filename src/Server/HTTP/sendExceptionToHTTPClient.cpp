#include <Server/HTTP/sendExceptionToHTTPClient.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>


namespace DB
{

void drainRequestIfNeeded(HTTPServerRequest & request, HTTPServerResponse & response) noexcept
{
    /// If HTTP method is POST and Keep-Alive is turned on, we should try to read the whole request body
    /// to avoid reading part of the current request body in the next request.
    /// Or we have to close connection after this request.
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
        && (request.getChunkedTransferEncoding() || request.hasContentLength())
        && response.getKeepAlive())
    {
        try
        {
            if (!request.getStream().eof())
                request.getStream().ignoreAll();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot read remaining request body during exception handling. Set keep alive to false on the response.");
            response.setKeepAlive(false);
        }
    }
}

}
