//
// TCPServerConnection.cpp
//
// $Id: //poco/1.4/Net/src/TCPServerConnection.cpp#1 $
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerConnection
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/TCPServerConnection.h"
#include "Poco/Exception.h"
#include "Poco/ErrorHandler.h"


using Poco::Exception;
using Poco::ErrorHandler;


namespace Poco {
namespace Net {


TCPServerConnection::TCPServerConnection(const StreamSocket& socket):
	_socket(socket)
{
}


TCPServerConnection::~TCPServerConnection()
{
}


void TCPServerConnection::start()
{
	try
	{
		run();
	}
	catch (Exception& exc)
	{
		ErrorHandler::handle(exc);
	}
	catch (std::exception& exc)
	{
		ErrorHandler::handle(exc);
	}
	catch (...)
	{
		ErrorHandler::handle();
	}
}


} } // namespace Poco::Net
