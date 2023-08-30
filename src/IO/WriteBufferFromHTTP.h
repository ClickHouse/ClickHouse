#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>


namespace DB
{

/* Perform HTTP POST/PUT request.
 */
class WriteBufferFromHTTP : public WriteBufferFromOStream
{
public:
    explicit WriteBufferFromHTTP(const Poco::URI & uri,
                                 const std::string & method = Poco::Net::HTTPRequest::HTTP_POST, // POST or PUT only
                                 const std::string & content_type = "",
                                 const std::string & content_encoding = "",
                                 const HTTPHeaderEntries & additional_headers = {},
                                 const ConnectionTimeouts & timeouts = {},
                                 size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
                                 Poco::Net::HTTPClientSession::ProxyConfig proxy_configuration = {});

private:
    /// Receives response from the server after sending all data.
    void finalizeImpl() override;

    HTTPSessionPtr session;
    Poco::Net::HTTPRequest request;
    Poco::Net::HTTPResponse response;
};

}
