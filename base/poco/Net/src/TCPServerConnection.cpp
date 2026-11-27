//
// TCPServerConnection.cpp
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


namespace Poco {
namespace Net {


TCPServerConnection::TCPServerConnection(const StreamSocket& socket):
	_socket(socket)
{
}


TCPServerConnection::~TCPServerConnection() = default;


} } // namespace Poco::Net
