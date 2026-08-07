#include <Server/HTTP/sendExceptionToHTTPClient.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

void drainRequestIfNeeded(HTTPServerRequest & request, HTTPServerResponse & response) noexcept
{
    auto input_stream = request.getStream();
    if (input_stream->isCanceled())
    {
        response.setKeepAlive(false);
        LOG_WARNING(getLogger("sendExceptionToHTTPClient"), "Cannot read remaining request body during exception handling. The request read buffer is canceled. Set keep alive to false on the response.");
        return;
    }

    LOG_DEBUG(getLogger("sendExceptionToHTTPClient"), "Draining connection ({}, Transfer-Encoding: {}, Content-Length: {}, Keep-Alive: {})",
              request.getVersion(), request.getTransferEncoding(), request.getContentLength(), request.getKeepAlive());

    /// If HTTP method is POST and Keep-Alive is turned on, we should try to read the whole request body
    /// to avoid reading part of the current request body in the next request.
    /// Or we have to close connection after this request.
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
        && (request.getChunkedTransferEncoding() || request.hasContentLength())
        && response.getKeepAlive())
    {
        /// If the client expects 100 Continue, but we never sent it, don't attempt to read the body and
        /// don't reuse the connection.
        if (request.getExpectContinue() && response.getStatus() != Poco::Net::HTTPResponse::HTTP_CONTINUE)
        {
            response.setKeepAlive(false);
        }
        else
        {
            try
            {
                if (!input_stream->eof())
                {
                    size_t ignored_bytes = input_stream->ignoreAll();
                    LOG_DEBUG(getLogger("sendExceptionToHTTPClient"), "Drained {} bytes", ignored_bytes);
                }
            }
            catch (...)
            {
                tryLogCurrentException("sendExceptionToHTTPClient", "Cannot read remaining request body during exception handling. Set keep alive to false on the response.");
                response.setKeepAlive(false);
            }
        }
    }
}

}
