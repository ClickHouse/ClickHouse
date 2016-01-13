//
// HTTPServerConnectionFactory.cpp
//
// $Id: //poco/1.4/Net/src/HTTPServerConnectionFactory.cpp#1 $
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerConnectionFactory
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPServerConnectionFactory.h"
#include "Poco/Net/HTTPServerConnection.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"


namespace Poco {
namespace Net {


HTTPServerConnectionFactory::HTTPServerConnectionFactory(HTTPServerParams::Ptr pParams, HTTPRequestHandlerFactory::Ptr pFactory):
	_pParams(pParams),
	_pFactory(pFactory)
{
	poco_check_ptr (pFactory);
}


HTTPServerConnectionFactory::~HTTPServerConnectionFactory()
{
}


TCPServerConnection* HTTPServerConnectionFactory::createConnection(const StreamSocket& socket)
{
	return new HTTPServerConnection(socket, _pParams, _pFactory);
}


} } // namespace Poco::Net
