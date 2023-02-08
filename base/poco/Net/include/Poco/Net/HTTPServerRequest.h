//
// HTTPServerRequest.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerRequest
//
// Definition of the HTTPServerRequest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPServerRequest_INCLUDED
#define Net_HTTPServerRequest_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/SocketAddress.h"
#include <istream>


namespace Poco {
namespace Net {


class HTTPServerSession;
class HTTPServerResponse;
class HTTPServerParams;


class Net_API HTTPServerRequest: public HTTPRequest
	/// This abstract subclass of HTTPRequest is used for
	/// representing server-side HTTP requests.
	///
	/// A HTTPServerRequest is passed to the
	/// handleRequest() method of HTTPRequestHandler.
{
public:
	HTTPServerRequest();
		/// Creates the HTTPServerRequest

	~HTTPServerRequest();
		/// Destroys the HTTPServerRequest.
		
	virtual std::istream& stream() = 0;
		/// Returns the input stream for reading
		/// the request body.
		///
		/// The stream must be valid until the HTTPServerRequest
		/// object is destroyed.
		
	virtual const SocketAddress& clientAddress() const = 0;
		/// Returns the client's address.

	virtual const SocketAddress& serverAddress() const = 0;
		/// Returns the server's address.
		
	virtual const HTTPServerParams& serverParams() const = 0;
		/// Returns a reference to the server parameters.

	virtual HTTPServerResponse& response() const = 0;
		/// Returns a reference to the associated response.
		
	virtual bool secure() const = 0;
		/// Returns true if the request is using a secure
		/// connection. Returns false if no secure connection
		/// is used, or if it is not known whether a secure
		/// connection is used.
};


} } // namespace Poco::Net


#endif // Net_HTTPServerRequest_INCLUDED
