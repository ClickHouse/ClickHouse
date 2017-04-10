#pragma once

#include <functional>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/URI.h>
#include <IO/ReadBuffer.h>

#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1

namespace DB
{

const int HTTP_TOO_MANY_REQUESTS = 429;

struct HTTPTimeouts
{
    Poco::Timespan connection_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, 0);
    Poco::Timespan send_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0);
    Poco::Timespan receive_timeout = Poco::Timespan(DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0);
};


/** Perform HTTP POST request and provide response to read.
  */
class ReadWriteBufferFromHTTP : public ReadBuffer
{
private:
    Poco::URI uri;
    std::string method;
    HTTPTimeouts timeouts;

    bool is_ssl;
    std::unique_ptr<Poco::Net::HTTPClientSession> session;
    std::istream * istr;    /// owned by session
    std::unique_ptr<ReadBuffer> impl;

public:
    using OutStreamCallback = std::function<void(std::ostream &)>;

    ReadWriteBufferFromHTTP(
        const Poco::URI & uri,
        const std::string & method = {},
        OutStreamCallback out_stream_callback = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const HTTPTimeouts & timeouts = {});

    bool nextImpl() override;
};

}
