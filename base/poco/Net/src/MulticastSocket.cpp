//
// MulticastSocket.cpp
//
// Library: Net
// Package: Sockets
// Module:  MulticastSocket
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MulticastSocket.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "Poco/Net/NetException.h"
#include <cstring>


#if defined(hpux) && defined(_XOPEN_SOURCE_EXTENDED) && defined(POCO_HPUX_IP_MREQ_HACK)
// netinet/in.h does not define struct ip_mreq if
// _XOPEN_SOURCE_EXTENDED is #define'd in HP-UX 11.x 
// versions prior to 11.30. Compile with -DPOCO_HPUX_IP_MREQ_HACK
// if you experience problems.
struct ip_mreq 
{
	struct in_addr imr_multiaddr;
	struct in_addr imr_interface;
};
#endif


// some Unix variants don't have IPV6_ADD_MEMBERSHIP/IPV6_DROP_MEMBERSHIP
#if defined(IPV6_JOIN_GROUP) && !defined(IPV6_ADD_MEMBERSHIP)
#define IPV6_ADD_MEMBERSHIP  IPV6_JOIN_GROUP
#define IPV6_DROP_MEMBERSHIP IPV6_LEAVE_GROUP
#endif


namespace Poco {
namespace Net {


MulticastSocket::MulticastSocket()
{
}


MulticastSocket::MulticastSocket(SocketAddress::Family family): DatagramSocket(family)
{
#if defined(POCO_OS_FAMILY_UNIX)
	if (family == SocketAddress::UNIX_LOCAL)
		throw Poco::InvalidArgumentException("Cannot create a MulticastSocket with UNIX_LOCAL socket");
#endif
}


MulticastSocket::MulticastSocket(const SocketAddress& address, bool reuseAddress): DatagramSocket(address, reuseAddress)
{
}


MulticastSocket::MulticastSocket(const Socket& socket): DatagramSocket(socket)
{
}


MulticastSocket::~MulticastSocket()
{
}


MulticastSocket& MulticastSocket::operator = (const Socket& socket)
{
	DatagramSocket::operator = (socket);
	return *this;
}


void MulticastSocket::setInterface(const NetworkInterface& interfc)
{
	if (address().family() == SocketAddress::IPv4)
	{
		impl()->setOption(IPPROTO_IP, IP_MULTICAST_IF, interfc.firstAddress(IPAddress::IPv4));
	}
#if defined(POCO_HAVE_IPv6)
	else if (address().family() == SocketAddress::IPv6)
	{
		impl()->setOption(IPPROTO_IPV6, IPV6_MULTICAST_IF, interfc.index());
	}
#endif
	else throw UnsupportedFamilyException("Unknown or unsupported socket family.");
}

	
NetworkInterface MulticastSocket::getInterface() const
{
	try
	{
		IPAddress addr;
		impl()->getOption(IPPROTO_IP, IP_MULTICAST_IF, addr);
		return NetworkInterface::forAddress(addr);
	}
	catch (Poco::Exception&)
	{
#if defined(POCO_HAVE_IPv6)
		int ix;
		impl()->getOption(IPPROTO_IPV6, IPV6_MULTICAST_IF, ix);
		return NetworkInterface::forIndex(ix);
#else
		throw;
#endif
	}
}

	
void MulticastSocket::setLoopback(bool flag)
{
	if (address().af() == AF_INET)
	{
		unsigned char uflag = flag ? 1 : 0;
		impl()->setOption(IPPROTO_IP, IP_MULTICAST_LOOP, uflag);
	}
	else
	{
#if defined(POCO_HAVE_IPv6)
		unsigned uflag = flag ? 1 : 0;
		impl()->setOption(IPPROTO_IPV6, IPV6_MULTICAST_LOOP, uflag);
#endif
	}
}

	
bool MulticastSocket::getLoopback() const
{
	bool flag = false;
	if (address().af() == AF_INET)
	{
		unsigned char uflag;
		impl()->getOption(IPPROTO_IP, IP_MULTICAST_LOOP, uflag);
		flag = uflag != 0;
	}
	else
	{
#if defined(POCO_HAVE_IPv6)
		unsigned uflag;
		impl()->getOption(IPPROTO_IPV6, IPV6_MULTICAST_LOOP, uflag);
		flag = uflag != 0;
#endif
	}
	return flag;
}

	
void MulticastSocket::setTimeToLive(unsigned value)
{
	if (address().af() == AF_INET)
	{
		unsigned char ttl = (unsigned char) value;
		impl()->setOption(IPPROTO_IP, IP_MULTICAST_TTL, ttl);
	}
	else
	{
#if defined(POCO_HAVE_IPv6)
		impl()->setOption(IPPROTO_IPV6, IPV6_MULTICAST_HOPS, value);
#endif
	}
}

	
unsigned MulticastSocket::getTimeToLive() const
{
	unsigned ttl(0);
	if (address().af() == AF_INET)
	{
		unsigned char cttl;
		impl()->getOption(IPPROTO_IP, IP_MULTICAST_TTL, cttl);
		ttl = cttl;
	}
	else
	{
#if defined(POCO_HAVE_IPv6)
		impl()->getOption(IPPROTO_IPV6, IPV6_MULTICAST_HOPS, ttl);
#endif
	}
	return ttl;
}

	
void MulticastSocket::joinGroup(const IPAddress& groupAddress)
{
	joinGroup(groupAddress, findFirstInterface(groupAddress));
}

	
void MulticastSocket::joinGroup(const IPAddress& groupAddress, const NetworkInterface& interfc)
{
	if (groupAddress.af() == AF_INET)
	{
		struct ip_mreq mr;
		std::memcpy(&mr.imr_multiaddr, groupAddress.addr(), groupAddress.length());
		std::memcpy(&mr.imr_interface, interfc.firstAddress(IPAddress::IPv4).addr(), interfc.firstAddress(IPAddress::IPv4).length());
		impl()->setRawOption(IPPROTO_IP, IP_ADD_MEMBERSHIP, &mr, sizeof(mr));
	}
	else
	{
#if defined(POCO_HAVE_IPv6)
		struct ipv6_mreq mr;
		std::memcpy(&mr.ipv6mr_multiaddr, groupAddress.addr(), groupAddress.length());
		mr.ipv6mr_interface = interfc.index();
		impl()->setRawOption(IPPROTO_IPV6, IPV6_ADD_MEMBERSHIP, &mr, sizeof(mr));
#endif
	}
}


NetworkInterface MulticastSocket::findFirstInterface(const IPAddress& groupAddress)
{
	NetworkInterface::Map m = NetworkInterface::map();
	if (groupAddress.family() == IPAddress::IPv4)
	{
		for (NetworkInterface::Map::const_iterator it = m.begin(); it != m.end(); ++it)
		{
			if (it->second.supportsIPv4() &&
				it->second.firstAddress(IPAddress::IPv4).isUnicast() &&
				!it->second.isLoopback() &&
				!it->second.isPointToPoint())
			{
				return it->second;
			}
		}
	}
#ifdef POCO_HAVE_IPv6
	else if (groupAddress.family() == IPAddress::IPv6)
	{
		for (NetworkInterface::Map::const_iterator it = m.begin(); it != m.end(); ++it)
		{
			if (it->second.supportsIPv6() &&
				it->second.firstAddress(IPAddress::IPv6).isUnicast() &&
				!it->second.isLoopback() &&
				!it->second.isPointToPoint())
			{
				return it->second;
			}
		}
	}
#endif // POCO_HAVE_IPv6

	throw NotFoundException("No multicast-eligible network interface found.");
}

	
void MulticastSocket::leaveGroup(const IPAddress& groupAddress)
{
	NetworkInterface intf;
	leaveGroup(groupAddress, intf);
}

	
void MulticastSocket::leaveGroup(const IPAddress& groupAddress, const NetworkInterface& interfc)
{
	if (groupAddress.af() == AF_INET)
	{
		struct ip_mreq mr;
		std::memcpy(&mr.imr_multiaddr, groupAddress.addr(), groupAddress.length());
		std::memcpy(&mr.imr_interface, interfc.firstAddress(IPAddress::IPv4).addr(), interfc.firstAddress(IPAddress::IPv4).length());
		impl()->setRawOption(IPPROTO_IP, IP_DROP_MEMBERSHIP, &mr, sizeof(mr));
	}
	else
	{
#if defined(POCO_HAVE_IPv6)
		struct ipv6_mreq mr;
		std::memcpy(&mr.ipv6mr_multiaddr, groupAddress.addr(), groupAddress.length());
		mr.ipv6mr_interface = interfc.index();
		impl()->setRawOption(IPPROTO_IPV6, IPV6_DROP_MEMBERSHIP, &mr, sizeof(mr));
#endif
	}
}


} } // namespace Poco::Net


#endif // POCO_NET_HAS_INTERFACE
