//
// HTTPServerResponse.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPServerResponse.h#1 $
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerResponse
//
// Definition of the HTTPServerResponse class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPServerResponse_INCLUDED
#define Net_HTTPServerResponse_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPResponse.h"
#include <cstddef>
#include <ostream>


namespace Poco {
namespace Net {


class HTTPServerSession;
class HTTPCookie;


class Net_API HTTPServerResponse: public HTTPResponse
	/// This subclass of HTTPResponse is used for
	/// representing server-side HTTP responses.
	///
	/// A HTTPServerResponse is passed to the
	/// handleRequest() method of HTTPRequestHandler.
	///
	/// handleRequest() must set a status code
	/// and optional reason phrase, set headers
	/// as necessary, and provide a message body.
{
public:
	HTTPServerResponse();
		/// Creates the HTTPServerResponse.

	~HTTPServerResponse();
		/// Destroys the HTTPServerResponse.

	virtual void sendContinue() = 0;
		/// Sends a 100 Continue response to the
		/// client.
		
	virtual std::ostream& send() = 0;
		/// Sends the response header to the client and
		/// returns an output stream for sending the
		/// response body.
		///
		/// The returned stream is valid until the response
		/// object is destroyed.
		///
		/// Must not be called after sendFile(), sendBuffer() 
		/// or redirect() has been called.
		
	virtual void sendFile(const std::string& path, const std::string& mediaType) = 0;
		/// Sends the response header to the client, followed
		/// by the content of the given file.
		///
		/// Must not be called after send(), sendBuffer() 
		/// or redirect() has been called.
		///
		/// Throws a FileNotFoundException if the file
		/// cannot be found, or an OpenFileException if
		/// the file cannot be opened.
		
	virtual void sendBuffer(const void* pBuffer, std::size_t length) = 0;
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
		
	virtual void redirect(const std::string& uri, HTTPStatus status = HTTP_FOUND) = 0;
		/// Sets the status code, which must be one of
		/// HTTP_MOVED_PERMANENTLY (301), HTTP_FOUND (302),
		/// or HTTP_SEE_OTHER (303),
		/// and sets the "Location" header field
		/// to the given URI, which according to
		/// the HTTP specification, must be absolute.
		///
		/// Must not be called after send() has been called.
		
	virtual void requireAuthentication(const std::string& realm) = 0;
		/// Sets the status code to 401 (Unauthorized)
		/// and sets the "WWW-Authenticate" header field
		/// according to the given realm.
		
	virtual bool sent() const = 0;
		/// Returns true if the response (header) has been sent.
};


} } // namespace Poco::Net


#endif // Net_HTTPServerResponse_INCLUDED
