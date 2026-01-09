#pragma once

#include <IO/ReadBuffer.h>
#include <Server/HTTP/HTTPRequest.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <config.h>

#include <Poco/Net/HTTPServerSession.h>

#include <memory>
#include <mutex>

namespace DB
{

class X509Certificate;

class HTTPServerRequest : public HTTPRequest
{
public:
    HTTPServerRequest(
        HTTPRequest request,
        ReadBufferPtr stream,
        bool stream_is_bounded,
        const Poco::Net::SocketAddress & client_address,
        const Poco::Net::SocketAddress & server_address,
        bool secure,
        Poco::Net::SocketImpl * socket);

    /// FIXME: it's a little bit inconvenient interface. The rationale is that all other ReadBuffer's wrap each other
    ///        via unique_ptr - but we can't inherit HTTPServerRequest from ReadBuffer and pass it around,
    ///        since we also need it in other places.

    /// Returns the input stream for reading the request body.
    ReadBufferPtr getStream() const
    {
        std::optional<std::lock_guard<std::mutex>> lock;
        if (get_stream_mutex.has_value())
            lock.emplace(get_stream_mutex.value());
        poco_check_ptr(stream);
        LOG_TEST(getLogger("HTTPServerRequest"), "Returning request input stream with ref count {}", stream.use_count());
        return stream;
    }

    bool checkPeerConnected() const;

    bool isSecure() const { return secure; }

    /// Returns the client's address.
    const Poco::Net::SocketAddress & clientAddress() const { return client_address; }

    /// Returns the server's address.
    const Poco::Net::SocketAddress & serverAddress() const { return server_address; }

#if USE_SSL
    bool havePeerCertificate() const;
    X509Certificate peerCertificate() const;
#endif

    bool canKeepAlive() const
    {
        /// FIXME: does it really need to be so complicated?
        //         I really want to move the keep-alive check to the HTTP1ServerConnection but currently it is hard
        if (!stream_is_bounded)
            return false;

        std::optional<std::lock_guard<std::mutex>> lock;
        if (get_stream_mutex.has_value())
            lock.emplace(get_stream_mutex.value());

        if (!stream)
            return true;

        if (stream.use_count() > 1)
        {
            LOG_ERROR(getLogger("HTTPServerRequest"), "Request stream is shared by multiple threads. HTTP keep alive is not possible. Use count {}", stream.use_count());
            return false;
        }
        else
        {
            LOG_TEST(getLogger("HTTPServerRequest"), "Request stream is not shared, can keep alive connection");
        }

        /// only this instance possesses the stream it is safe to read from it
        return !stream->isCanceled() && stream->eof();
    }

    std::string toStringForLogging() const;

private:
    mutable std::optional<std::mutex> get_stream_mutex;
    ReadBufferPtr stream;
    const bool stream_is_bounded = true;
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;

    const bool secure = false;

    /// FIXME: this field should not exist because we want this class to represent an abstract HTTP request
    Poco::Net::SocketImpl * socket;
};

}
