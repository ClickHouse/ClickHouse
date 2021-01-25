#pragma once

#include <IO/ReadBuffer.h>
#include <Server/HTTP/HTTPRequest.h>

#include <Poco/Net/HTTPServerSession.h>

namespace DB
{

class HTTPServerResponse;

class HTTPServerRequest : public HTTPRequest
{
public:
    HTTPServerRequest(HTTPServerResponse & response, Poco::Net::HTTPServerSession & session);

    /// FIXME: it's a little bit inconvenient interface. The rationale is that all other ReadBuffer's wrap each other
    ///        via unique_ptr - but we can't inherit HTTPServerRequest from ReadBuffer and pass it around,
    ///        since we also need it in other places.

    /// Returns the input stream for reading the request body.
    ReadBuffer & getStream()
	{
		poco_check_ptr(stream);
	    return *stream;
	}

	/// Returns the client's address.
    const Poco::Net::SocketAddress & clientAddress() const { return client_address; }

	/// Returns the server's address.
    const Poco::Net::SocketAddress & serverAddress() const { return server_address; }

private:
    std::unique_ptr<ReadBuffer> stream;
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;
};

}
