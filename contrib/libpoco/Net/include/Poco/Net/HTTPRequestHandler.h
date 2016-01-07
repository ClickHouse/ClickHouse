//
// HTTPRequestHandler.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPRequestHandler.h#1 $
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPRequestHandler
//
// Definition of the HTTPRequestHandler class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPRequestHandler_INCLUDED
#define Net_HTTPRequestHandler_INCLUDED


#include "Poco/Net/Net.h"


namespace Poco {
namespace Net {


class HTTPServerRequest;
class HTTPServerResponse;


class Net_API HTTPRequestHandler
	/// The abstract base class for HTTPRequestHandlers 
	/// created by HTTPServer.
	///
	/// Derived classes must override the handleRequest() method.
	/// Furthermore, a HTTPRequestHandlerFactory must be provided.
	///
	/// The handleRequest() method must perform the complete handling
	/// of the HTTP request connection. As soon as the handleRequest() 
	/// method returns, the request handler object is destroyed.
	///
	/// A new HTTPRequestHandler object will be created for
	/// each new HTTP request that is received by the HTTPServer.
{
public:
	HTTPRequestHandler();
		/// Creates the HTTPRequestHandler.

	virtual ~HTTPRequestHandler();
		/// Destroys the HTTPRequestHandler.

	virtual void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) = 0;
		/// Must be overridden by subclasses.
		///
		/// Handles the given request.

private:
	HTTPRequestHandler(const HTTPRequestHandler&);
	HTTPRequestHandler& operator = (const HTTPRequestHandler&);
};


} } // namespace Poco::Net


#endif // Net_HTTPRequestHandler_INCLUDED
