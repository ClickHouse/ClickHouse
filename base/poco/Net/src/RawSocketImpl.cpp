//
// RawSocketImpl.cpp
//
// Library: Net
// Package: Sockets
// Module:  RawSocketImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/RawSocketImpl.h"
#include "Poco/Net/NetException.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


RawSocketImpl::RawSocketImpl()
{
	init(AF_INET);
}


RawSocketImpl::RawSocketImpl(SocketAddress::Family family, int proto)
{
	if (family == SocketAddress::IPv4)
		init2(AF_INET, proto);
#if defined(POCO_HAVE_IPv6)
	else if (family == SocketAddress::IPv6)
		init2(AF_INET6, proto);
#endif
	else throw InvalidArgumentException("Invalid or unsupported address family passed to RawSocketImpl");

}

	
RawSocketImpl::RawSocketImpl(poco_socket_t sockfd): 
	SocketImpl(sockfd)
{
}


RawSocketImpl::~RawSocketImpl()
{
}


void RawSocketImpl::init(int af)
{
	init2(af, IPPROTO_RAW);
}


void RawSocketImpl::init2(int af, int proto)
{
	initSocket(af, SOCK_RAW, proto);
	setOption(IPPROTO_IP, IP_HDRINCL, 0);
}


} } // namespace Poco::Net
