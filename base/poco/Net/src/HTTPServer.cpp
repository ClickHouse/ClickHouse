//
// HTTPServer.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServer
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPServerConnectionFactory.h"


namespace Poco {
namespace Net {


HTTPServer::HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, Poco::UInt16 portNumber, HTTPServerParams::Ptr pParams):
	TCPServer(new HTTPServerConnectionFactory(pParams, pFactory), portNumber, pParams),
	_pFactory(pFactory)
{
}


HTTPServer::HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, const ServerSocket& socket, HTTPServerParams::Ptr pParams):
	TCPServer(new HTTPServerConnectionFactory(pParams, pFactory), socket, pParams),
	_pFactory(pFactory)
{
}


HTTPServer::HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, Poco::ThreadPool& threadPool, const ServerSocket& socket, HTTPServerParams::Ptr pParams):
	TCPServer(new HTTPServerConnectionFactory(pParams, pFactory), threadPool, socket, pParams),
	_pFactory(pFactory)
{
}


HTTPServer::~HTTPServer()
{
	/// We should call stop and join thread here instead of destructor of parent TCPHandler,
	/// because there's possible race on 'vptr' between this virtual destructor and 'run' method.
	stop();
}


void HTTPServer::stopAll(bool abortCurrent)
{
	stop();
	_pFactory->serverStopped(this, abortCurrent);
}


} } // namespace Poco::Net
