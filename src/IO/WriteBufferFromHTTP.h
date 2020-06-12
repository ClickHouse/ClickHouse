#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/HTTPCommon.h>
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
private:
    HTTPSessionPtr session;
    Poco::Net::HTTPRequest request;
    Poco::Net::HTTPResponse response;

public:
    explicit WriteBufferFromHTTP(const Poco::URI & uri,
        const std::string & method = Poco::Net::HTTPRequest::HTTP_POST, // POST or PUT only
        const ConnectionTimeouts & timeouts = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    /// Receives response from the server after sending all data.
    void finalize() override;
};

}
