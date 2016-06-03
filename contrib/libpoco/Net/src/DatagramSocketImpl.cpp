//
// DatagramSocketImpl.cpp
//
// $Id: //poco/1.4/Net/src/DatagramSocketImpl.cpp#1 $
//
// Library: Net
// Package: Sockets
// Module:  DatagramSocketImpl
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/DatagramSocketImpl.h"
#include "Poco/Net/NetException.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


DatagramSocketImpl::DatagramSocketImpl()
{
	init(AF_INET);
}


DatagramSocketImpl::DatagramSocketImpl(IPAddress::Family family)
{
	if (family == IPAddress::IPv4)
		init(AF_INET);
#if defined(POCO_HAVE_IPv6)
	else if (family == IPAddress::IPv6)
		init(AF_INET6);
#endif
	else throw InvalidArgumentException("Invalid or unsupported address family passed to DatagramSocketImpl");
}

	
DatagramSocketImpl::DatagramSocketImpl(poco_socket_t sockfd): SocketImpl(sockfd)
{
}


DatagramSocketImpl::~DatagramSocketImpl()
{
}


void DatagramSocketImpl::init(int af)
{
	initSocket(af, SOCK_DGRAM);
}


} } // namespace Poco::Net
