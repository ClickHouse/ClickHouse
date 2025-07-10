#include <Server/HTTP/sendExceptionToHTTPClient.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

void drainRequestIfNeeded(HTTPServerRequest & request, HTTPServerResponse & response) noexcept
{
    if (request.getStream().isCanceled())
    {
        response.setKeepAlive(false);
        LOG_WARNING(getLogger("sendExceptionToHTTPClient"), "Cannot read remaining request body during exception handling. The request read buffer is canceled. Set keep alive to false on the response.");
        return;
    }

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
            tryLogCurrentException("sendExceptionToHTTPClient", "Cannot read remaining request body during exception handling. Set keep alive to false on the response.");
            response.setKeepAlive(false);
        }
    }
}

}
