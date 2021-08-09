#pragma once

#include <Server/HTTP/HTTPResponse.h>

#include <Poco/Net/HTTPServerSession.h>
#include <Poco/Net/HTTPResponse.h>

#include <iostream>
#include <memory>

namespace DB
{

class HTTPServerRequest;

class HTTPServerResponse : public HTTPResponse
{
public:
    explicit HTTPServerResponse(Poco::Net::HTTPServerSession & session);

    void sendContinue(); /// Sends a 100 Continue response to the client.

    /// Sends the response header to the client and
    /// returns an output stream for sending the
    /// response body.
    ///
    /// Must not be called after beginSend(), sendFile(), sendBuffer()
    /// or redirect() has been called.
    std::shared_ptr<std::ostream> send(); /// TODO: use some WriteBuffer implementation here.

    /// Sends the response headers to the client
    /// but do not finish headers with \r\n,
    /// allowing to continue sending additional header fields.
    ///
    /// Must not be called after send(), sendFile(), sendBuffer()
    /// or redirect() has been called.
    std::pair<std::shared_ptr<std::ostream>, std::shared_ptr<std::ostream>> beginSend(); /// TODO: use some WriteBuffer implementation here.

    /// Sends the response header to the client, followed
    /// by the contents of the given buffer.
    ///
    /// The Content-Length header of the response is set
    /// to length and chunked transfer encoding is disabled.
    ///
    /// If both the HTTP message header and body (from the
    /// given buffer) fit into one single network packet, the
    /// complete response can be sent in one network packet.
    ///
    /// Must not be called after send(), sendFile()
    /// or redirect() has been called.
    void sendBuffer(const void * pBuffer, std::size_t length); /// FIXME: do we need this one?

    void requireAuthentication(const std::string & realm);
    /// Sets the status code to 401 (Unauthorized)
    /// and sets the "WWW-Authenticate" header field
    /// according to the given realm.

    /// Returns true if the response (header) has been sent.
    bool sent() const { return !!stream; }

    void attachRequest(HTTPServerRequest * request_) { request = request_; }

private:
    Poco::Net::HTTPServerSession & session;
    HTTPServerRequest * request = nullptr;
    std::shared_ptr<std::ostream> stream;
    std::shared_ptr<std::ostream> header_stream;
};

}
