#pragma once

#include <functional>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/URI.h>
#include <IO/ReadBuffer.h>
#include <IO/ConnectionTimeouts.h>

#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1

namespace DB
{

/** Perform HTTP POST request and provide response to read.
  */
class ReadWriteBufferFromHTTP : public ReadBuffer
{
private:
    Poco::URI uri;
    std::string method;

    std::unique_ptr<Poco::Net::HTTPClientSession> session;
    std::istream * istr;    /// owned by session
    std::unique_ptr<ReadBuffer> impl;

public:
    using OutStreamCallback = std::function<void(std::ostream &)>;

    explicit ReadWriteBufferFromHTTP(
        const Poco::URI & uri,
        const std::string & method = {},
        OutStreamCallback out_stream_callback = {},
        const ConnectionTimeouts & timeouts = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;
};

}
