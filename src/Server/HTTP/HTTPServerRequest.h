#pragma once

#include <IO/ReadBuffer.h>
#include <Server/HTTP/HTTPRequest.h>
#include <Common/ProfileEvents.h>
#include "config.h"

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/SocketImpl.h>

namespace DB
{

class X509Certificate;

class HTTPServerRequest : public HTTPRequest
{
public:
    HTTPServerRequest(
        HTTPRequest request,
        std::unique_ptr<ReadBuffer> body,
        const Poco::Net::SocketAddress & client_address,
        const Poco::Net::SocketAddress & server_address,
        bool secure,
        Poco::Net::SocketImpl * socket);

    /// FIXME: it's a little bit inconvenient interface. The rationale is that all other ReadBuffer's wrap each other
    ///        via unique_ptr - but we can't inherit HTTPServerRequest from ReadBuffer and pass it around,
    ///        since we also need it in other places.

    ReadBuffer & getStream() const
    {
        poco_check_ptr(body);
        return *body;
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

private:
    std::unique_ptr<ReadBuffer> body;
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;
    
    const bool secure;

    /// FIXME: this field should not exist
    Poco::Net::SocketImpl * socket;
};

}
