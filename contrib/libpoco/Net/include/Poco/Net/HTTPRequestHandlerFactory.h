//
// HTTPRequestHandlerFactory.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPRequestHandlerFactory.h#1 $
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPRequestHandlerFactory
//
// Definition of the HTTPRequestHandlerFactory class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPRequestHandlerFactory_INCLUDED
#define Net_HTTPRequestHandlerFactory_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/SharedPtr.h"
#include "Poco/BasicEvent.h"


namespace Poco {
namespace Net {


class HTTPServerRequest;
class HTTPServerResponse;
class HTTPRequestHandler;


class Net_API HTTPRequestHandlerFactory
	/// A factory for HTTPRequestHandler objects.
	/// Subclasses must override the createRequestHandler()
	/// method.
{
public:
	typedef Poco::SharedPtr<HTTPRequestHandlerFactory> Ptr;
	
	HTTPRequestHandlerFactory();
		/// Creates the HTTPRequestHandlerFactory.

	virtual ~HTTPRequestHandlerFactory();
		/// Destroys the HTTPRequestHandlerFactory.

	virtual HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request) = 0;
		/// Must be overridden by sublasses.
		///
		/// Creates a new request handler for the given HTTP request.

protected:
	Poco::BasicEvent<const bool> serverStopped;

private:
	HTTPRequestHandlerFactory(const HTTPRequestHandlerFactory&);
	HTTPRequestHandlerFactory& operator = (const HTTPRequestHandlerFactory&);
	
	friend class HTTPServer;
	friend class HTTPServerConnection;
};


} } // namespace Poco::Net


#endif // Net_HTTPRequestHandlerFactory_INCLUDED
