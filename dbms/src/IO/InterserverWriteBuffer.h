#pragma once

#include <IO/WriteBuffer.h>
#include <IO/HashingWriteBuffer.h>

#include <Poco/Net/HTTPClientSession.h>

namespace DB
{

namespace
{

constexpr auto DEFAULT_REMOTE_WRITE_BUFFER_CONNECTION_TIMEOUT = 1;
constexpr auto DEFAULT_REMOTE_WRITE_BUFFER_RECEIVE_TIMEOUT = 1800;
constexpr auto DEFAULT_REMOTE_WRITE_BUFFER_SEND_TIMEOUT = 1800;

}

/** Allows you to write a file to a remote server.
  */
class InterserverWriteBuffer final : public WriteBuffer
{
public:
    InterserverWriteBuffer(const std::string & host_, int port_,
        const std::string & endpoint_,
        const std::string & path_,
        bool compress_ = false,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const Poco::Timespan & connection_timeout = Poco::Timespan(DEFAULT_REMOTE_WRITE_BUFFER_CONNECTION_TIMEOUT, 0),
        const Poco::Timespan & send_timeout = Poco::Timespan(DEFAULT_REMOTE_WRITE_BUFFER_SEND_TIMEOUT, 0),
        const Poco::Timespan & receive_timeout = Poco::Timespan(DEFAULT_REMOTE_WRITE_BUFFER_RECEIVE_TIMEOUT, 0));

    ~InterserverWriteBuffer();
    void finalize();
    void cancel();

private:
    void nextImpl() override;

private:
    std::string host;
    int port;
    std::string path;

    Poco::Net::HTTPClientSession session;
    std::ostream * ostr;    /// this is owned by session
    std::unique_ptr<WriteBuffer> impl;

    /// Sent all the data and renamed the file
    bool finalized = false;
};

}
