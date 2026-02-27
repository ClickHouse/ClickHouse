#pragma once

#include <Interpreters/Context_fwd.h>
#include <IO/ReadBuffer.h>
#include <Server/HTTP/HTTPRequest.h>
#include <Server/HTTP/HTTPContext.h>
#include <Common/StackTrace.h>
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
class HTTPServerResponse;
class ReadBufferFromPocoSocket;

class HTTPServerRequest : public HTTPRequest
{
public:
    HTTPServerRequest(HTTPContextPtr context, HTTPServerResponse & response, Poco::Net::HTTPServerSession & session, const ProfileEvents::Event & read_event = ProfileEvents::end());

    /// FIXME: it's a little bit inconvenient interface. The rationale is that all other ReadBuffer's wrap each other
    ///        via unique_ptr - but we can't inherit HTTPServerRequest from ReadBuffer and pass it around,
    ///        since we also need it in other places.

    /// Returns the input stream for reading the request body.
    ReadBufferPtr getStream()
    {
        std::lock_guard lock(get_stream_mutex);
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
        std::lock_guard lock(get_stream_mutex);

        if (!stream)
            return true;

        if (!stream_is_bounded)
            return false;

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

private:
    /// Limits for basic sanity checks when reading a header
    enum Limits
    {
        MAX_METHOD_LENGTH = 32,
        MAX_VERSION_LENGTH = 8,
    };

    const size_t max_uri_size;
    const size_t max_fields_number;
    const size_t max_field_name_size;
    const size_t max_field_value_size;

    mutable std::mutex get_stream_mutex;
    ReadBufferPtr stream TSA_GUARDED_BY(get_stream_mutex);
    Poco::Net::SocketImpl * socket;
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;

    bool stream_is_bounded = false;
    bool secure;

    void readRequest(ReadBuffer & in);
};

}
