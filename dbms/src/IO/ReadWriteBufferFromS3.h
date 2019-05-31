#pragma once

#include <functional>
#include <memory>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Version.h>
#include <Common/DNSResolver.h>
#include <Common/config.h>
#include <common/logger_useful.h>


#define DEFAULT_S3_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_S3_READ_BUFFER_CONNECTION_TIMEOUT 1
#define DEFAULT_S3_MAX_FOLLOW_REDIRECT 2

namespace DB
{
/** Perform S3 HTTP POST request and provide response to read.
  */

class ReadWriteBufferFromS3 : public ReadBuffer
{
protected:
    Poco::URI uri;
    std::string method;

    HTTPSessionPtr session;
    std::istream * istr; /// owned by session
    std::unique_ptr<ReadBuffer> impl;

public:
    using OutStreamCallback = std::function<void(std::ostream &)>;

    explicit ReadWriteBufferFromS3(Poco::URI uri_,
        const std::string & method_ = {},
        OutStreamCallback out_stream_callback = {},
        const ConnectionTimeouts & timeouts = {},
        const Poco::Net::HTTPBasicCredentials & credentials = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;
};


/* Perform S3 HTTP POST/PUT request.
 */
class WriteBufferFromS3 : public WriteBufferFromOStream
{
private:
    HTTPSessionPtr session;
    Poco::Net::HTTPRequest request;
    Poco::Net::HTTPResponse response;

public:
    explicit WriteBufferFromS3(const Poco::URI & uri,
        const std::string & method = Poco::Net::HTTPRequest::HTTP_POST, // POST for inserting, PUT for replacing.
        const ConnectionTimeouts & timeouts = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    /// Receives response from the server after sending all data.
    void finalize();
};

}
