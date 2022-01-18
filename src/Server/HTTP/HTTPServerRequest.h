#pragma once

#include <Interpreters/Context_fwd.h>
#include <IO/ReadBuffer.h>
#include <Server/HTTP/HTTPRequest.h>

#include <Poco/Net/HTTPServerSession.h>

namespace DB
{

class HTTPServerResponse;
class ReadBufferFromPocoSocket;

class HTTPServerRequest : public HTTPRequest
{
public:
    HTTPServerRequest(ContextPtr context, HTTPServerResponse & response, Poco::Net::HTTPServerSession & session);

    /// FIXME: it's a little bit inconvenient interface. The rationale is that all other ReadBuffer's wrap each other
    ///        via unique_ptr - but we can't inherit HTTPServerRequest from ReadBuffer and pass it around,
    ///        since we also need it in other places.

    /// Returns the input stream for reading the request body.
    ReadBuffer & getStream()
    {
        poco_check_ptr(stream);
        return *stream;
    }

    bool checkPeerConnected() const;

    /// Returns the client's address.
    const Poco::Net::SocketAddress & clientAddress() const { return client_address; }

    /// Returns the server's address.
    const Poco::Net::SocketAddress & serverAddress() const { return server_address; }

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

    std::unique_ptr<ReadBuffer> stream;
    Poco::Net::SocketImpl * socket;
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;

    void readRequest(ReadBuffer & in);
};

}
