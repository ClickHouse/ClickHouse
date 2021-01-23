#pragma once

#include <Server/HTTP/HTTPRequest.h>

#include <Poco/Net/HTTPServerSession.h>

namespace DB
{

class HTTPServerResponse;

class HTTPServerRequest : public HTTPRequest
{
public:
    HTTPServerRequest(HTTPServerResponse & response, Poco::Net::HTTPServerSession & session, Poco::Net::HTTPServerParams * params);

    /// Returns the input stream for reading the request body.
    std::istream & getStream()  /// TODO: replace with some implementation of ReadBuffer
	{
		poco_check_ptr(stream);
	    return *stream;
	}

	/// Returns the client's address.
    const Poco::Net::SocketAddress & clientAddress() const { return client_address; }

	/// Returns the server's address.
    const Poco::Net::SocketAddress & serverAddress() const { return server_address; }

	/// Returns a reference to the server parameters.
    const Poco::Net::HTTPServerParams & serverParams() const { return *params; }

	/// Returns a reference to the associated response.
    HTTPServerResponse & getResponse() const { return response; }

	/// Returns true if the request is using a secure
    /// connection. Returns false if no secure connection
    /// is used, or if it is not known whether a secure
    /// connection is used.
    bool secure() const { return session.socket().secure(); }

	/// Returns a reference to the underlying socket.
    Poco::Net::StreamSocket & socket() { return session.socket(); }

	/// Returns the underlying socket after detaching
    /// it from the server session.
    Poco::Net::StreamSocket detachSocket() { return session.detachSocket(); }

	/// Returns the underlying HTTPServerSession.
    Poco::Net::HTTPServerSession & getSession() { return session; }

private:
    HTTPServerResponse & response;
    Poco::Net::HTTPServerSession & session;
    std::unique_ptr<std::istream> stream;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> params;  // since |HTTPServerParams| is internally ref-counted, we have to leave it as such.
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;
};

}
