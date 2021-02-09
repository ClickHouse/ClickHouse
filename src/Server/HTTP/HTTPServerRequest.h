#pragma once

#include <IO/ReadBuffer.h>
#include <Server/HTTP/HTTPRequest.h>

#include <Poco/Net/HTTPServerSession.h>

namespace DB
{

class Context;
class HTTPServerResponse;
class ReadBufferFromPocoSocket;

class HTTPServerRequest : public HTTPRequest
{
public:
    HTTPServerRequest(const Context & context, HTTPServerResponse & response, Poco::Net::HTTPServerSession & session);

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
        MAX_NAME_LENGTH = 256,
        MAX_VALUE_LENGTH = 8192,
        MAX_METHOD_LENGTH = 32,
        MAX_URI_LENGTH = 16384,
        MAX_VERSION_LENGTH = 8,
        MAX_FIELDS_NUMBER = 100,
    };

    std::unique_ptr<ReadBuffer> stream;
    Poco::Net::SocketImpl * socket;
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;

    void readRequest(ReadBuffer & in);
};

}
