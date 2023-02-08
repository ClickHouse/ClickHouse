//
// SocketAddress.cpp
//
// Library: Net
// Package: NetCore
// Module:  SocketAddress
//
// Copyright (c) 2005-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/IPAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/DNS.h"
#include "Poco/RefCountedObject.h"
#include "Poco/NumberParser.h"
#include "Poco/BinaryReader.h"
#include "Poco/BinaryWriter.h"
#include <algorithm>
#include <cstring>


using Poco::RefCountedObject;
using Poco::NumberParser;
using Poco::UInt16;
using Poco::InvalidArgumentException;
using Poco::Net::Impl::SocketAddressImpl;
using Poco::Net::Impl::IPv4SocketAddressImpl;
#ifdef POCO_HAVE_IPv6
using Poco::Net::Impl::IPv6SocketAddressImpl;
#endif
#ifdef POCO_OS_FAMILY_UNIX
using Poco::Net::Impl::LocalSocketAddressImpl;
#endif


namespace Poco {
namespace Net {


struct AFLT
{
	bool operator () (const IPAddress& a1, const IPAddress& a2)
	{
		return a1.af() < a2.af();
	}
};


//
// SocketAddress
//


#if !defined(_MSC_VER) || defined(__STDC__)
// Go home MSVC, you're drunk...
// See http://stackoverflow.com/questions/5899857/multiple-definition-error-for-static-const-class-members
const SocketAddress::Family SocketAddress::IPv4;
#if defined(POCO_HAVE_IPv6)
const SocketAddress::Family SocketAddress::IPv6;
#endif
#if defined(POCO_OS_FAMILY_UNIX)
const SocketAddress::Family SocketAddress::UNIX_LOCAL;
#endif
#endif


SocketAddress::SocketAddress()
{
	newIPv4();
}


SocketAddress::SocketAddress(Family fam)
{
		init(IPAddress(fam), 0);
}


SocketAddress::SocketAddress(const IPAddress& hostAddress, Poco::UInt16 portNumber)
{
	init(hostAddress, portNumber);
}


SocketAddress::SocketAddress(Poco::UInt16 portNumber)
{
	init(IPAddress(), portNumber);
}


SocketAddress::SocketAddress(Family fam, Poco::UInt16 portNumber)
{
	init(IPAddress(fam), portNumber);
}


SocketAddress::SocketAddress(const std::string& hostAddress, Poco::UInt16 portNumber)
{
	init(hostAddress, portNumber);
}


SocketAddress::SocketAddress(Family fam, const std::string& hostAddress, Poco::UInt16 portNumber)
{
	init(fam, hostAddress, portNumber);
}


SocketAddress::SocketAddress(const std::string& hostAddress, const std::string& portNumber)
{
	init(hostAddress, resolveService(portNumber));
}


SocketAddress::SocketAddress(Family fam, const std::string& hostAddress, const std::string& portNumber)
{
	init(fam, hostAddress, resolveService(portNumber));
}


SocketAddress::SocketAddress(Family fam, const std::string& addr)
{
	init(fam, addr);
}


SocketAddress::SocketAddress(const std::string& hostAndPort)
{
	init(hostAndPort);
}


SocketAddress::SocketAddress(const SocketAddress& socketAddress)
{
	if (socketAddress.family() == IPv4)
		newIPv4(reinterpret_cast<const sockaddr_in*>(socketAddress.addr()));
#if defined(POCO_HAVE_IPv6)
	else if (socketAddress.family() == IPv6)
		newIPv6(reinterpret_cast<const sockaddr_in6*>(socketAddress.addr()));
#endif
#if defined(POCO_OS_FAMILY_UNIX)
	else if (socketAddress.family() == UNIX_LOCAL)
		newLocal(reinterpret_cast<const sockaddr_un*>(socketAddress.addr()));
#endif
}


SocketAddress::SocketAddress(const struct sockaddr* sockAddr, poco_socklen_t length)
{
	if (length == sizeof(struct sockaddr_in) && sockAddr->sa_family == AF_INET)
		newIPv4(reinterpret_cast<const struct sockaddr_in*>(sockAddr));
#if defined(POCO_HAVE_IPv6)
	else if (length == sizeof(struct sockaddr_in6) && sockAddr->sa_family == AF_INET6)
		newIPv6(reinterpret_cast<const struct sockaddr_in6*>(sockAddr));
#endif
#if defined(POCO_OS_FAMILY_UNIX)
	else if (length > 0 && length <= sizeof(struct sockaddr_un) && sockAddr->sa_family == AF_UNIX)
		newLocal(reinterpret_cast<const sockaddr_un*>(sockAddr));
#endif
	else throw Poco::InvalidArgumentException("Invalid address length or family passed to SocketAddress()");
}


SocketAddress::~SocketAddress()
{
}


bool SocketAddress::operator < (const SocketAddress& socketAddress) const
{
	if (family() < socketAddress.family()) return true;
	if (family() > socketAddress.family()) return false;
#if defined(POCO_OS_FAMILY_UNIX)
	if (family() == UNIX_LOCAL) return toString() < socketAddress.toString();
#endif
	if (host() < socketAddress.host()) return true;
	if (host() > socketAddress.host()) return false;
	return (port() < socketAddress.port());
}


SocketAddress& SocketAddress::operator = (const SocketAddress& socketAddress)
{
	if (&socketAddress != this)
	{
		if (socketAddress.family() == IPv4)
			newIPv4(reinterpret_cast<const sockaddr_in*>(socketAddress.addr()));
#if defined(POCO_HAVE_IPv6)
		else if (socketAddress.family() == IPv6)
			newIPv6(reinterpret_cast<const sockaddr_in6*>(socketAddress.addr()));
#endif
#if defined(POCO_OS_FAMILY_UNIX)
		else if (socketAddress.family() == UNIX_LOCAL)
			newLocal(reinterpret_cast<const sockaddr_un*>(socketAddress.addr()));
#endif
	}
	return *this;
}


IPAddress SocketAddress::host() const
{
	return pImpl()->host();
}


Poco::UInt16 SocketAddress::port() const
{
	return ntohs(pImpl()->port());
}


poco_socklen_t SocketAddress::length() const
{
	return pImpl()->length();
}


const struct sockaddr* SocketAddress::addr() const
{
	return pImpl()->addr();
}


int SocketAddress::af() const
{
	return pImpl()->af();
}


SocketAddress::Family SocketAddress::family() const
{
	return static_cast<Family>(pImpl()->family());
}


std::string SocketAddress::toString() const
{
	return pImpl()->toString();
}


void SocketAddress::init(const IPAddress& hostAddress, Poco::UInt16 portNumber)
{
	if (hostAddress.family() == IPAddress::IPv4)
		newIPv4(hostAddress, portNumber);
#if defined(POCO_HAVE_IPv6)
	else if (hostAddress.family() == IPAddress::IPv6)
		newIPv6(hostAddress, portNumber);
#endif
	else throw Poco::NotImplementedException("unsupported IP address family");
}


void SocketAddress::init(const std::string& hostAddress, Poco::UInt16 portNumber)
{
	IPAddress ip;
	if (IPAddress::tryParse(hostAddress, ip))
	{
		init(ip, portNumber);
	}
	else
	{
		HostEntry he = DNS::hostByName(hostAddress);
		HostEntry::AddressList addresses = he.addresses();
		if (addresses.size() > 0)
		{
#if defined(POCO_HAVE_IPv6) && defined(POCO_SOCKETADDRESS_PREFER_IPv4)
			// if we get both IPv4 and IPv6 addresses, prefer IPv4
			std::stable_sort(addresses.begin(), addresses.end(), AFLT());
#endif
			init(addresses[0], portNumber);
		}
		else throw HostNotFoundException("No address found for host", hostAddress);
	}
}


void SocketAddress::init(Family fam, const std::string& hostAddress, Poco::UInt16 portNumber)
{
	IPAddress ip;
	if (IPAddress::tryParse(hostAddress, ip))
	{
		if (ip.family() != fam) throw AddressFamilyMismatchException(hostAddress);
		init(ip, portNumber);
	}
	else
	{
		HostEntry he = DNS::hostByName(hostAddress);
		HostEntry::AddressList addresses = he.addresses();
		if (addresses.size() > 0)
		{
			for (HostEntry::AddressList::const_iterator it = addresses.begin(); it != addresses.end(); ++it)
			{
				if (it->family() == fam)
				{
					init(*it, portNumber);
					return;
				}
			}
			throw AddressFamilyMismatchException(hostAddress);
		}
		else throw HostNotFoundException("No address found for host", hostAddress);
	}
}


void SocketAddress::init(Family fam, const std::string& address)
{
#if defined(POCO_OS_FAMILY_UNIX)
	if (fam == UNIX_LOCAL)
	{
		newLocal(address);
	}
	else
#endif
	{
		std::string host;
		std::string port;
		std::string::const_iterator it  = address.begin();
		std::string::const_iterator end = address.end();

		if (*it == '[')
		{
			++it;
			while (it != end && *it != ']') host += *it++;
			if (it == end) throw InvalidArgumentException("Malformed IPv6 address");
			++it;
		}
		else
		{
			while (it != end && *it != ':') host += *it++;
		}
		if (it != end && *it == ':')
		{
			++it;
			while (it != end) port += *it++;
		}
		else throw InvalidArgumentException("Missing port number");
		init(fam, host, resolveService(port));
	}
}


void SocketAddress::init(const std::string& hostAndPort)
{
	poco_assert (!hostAndPort.empty());

	std::string host;
	std::string port;
	std::string::const_iterator it  = hostAndPort.begin();
	std::string::const_iterator end = hostAndPort.end();

#if defined(POCO_OS_FAMILY_UNIX)
	if (*it == '/')
	{
		newLocal(hostAndPort);
		return;
	}
#endif
	if (*it == '[')
	{
		++it;
		while (it != end && *it != ']') host += *it++;
		if (it == end) throw InvalidArgumentException("Malformed IPv6 address");
		++it;
	}
	else
	{
		while (it != end && *it != ':') host += *it++;
	}
	if (it != end && *it == ':')
	{
		++it;
		while (it != end) port += *it++;
	}
	else throw InvalidArgumentException("Missing port number");
	init(host, resolveService(port));
}


Poco::UInt16 SocketAddress::resolveService(const std::string& service)
{
	unsigned port;
	if (NumberParser::tryParseUnsigned(service, port) && port <= 0xFFFF)
	{
		return (UInt16) port;
	}
	else
	{
#if defined(POCO_VXWORKS)
		throw ServiceNotFoundException(service);
#else
		struct servent* se = getservbyname(service.c_str(), NULL);
		if (se)
			return ntohs(se->s_port);
		else
			throw ServiceNotFoundException(service);
#endif
	}
}


} } // namespace Poco::Net


Poco::BinaryWriter& operator << (Poco::BinaryWriter& writer, const Poco::Net::SocketAddress& value)
{
	writer << value.host();
	writer << value.port();
	return writer;
}


Poco::BinaryReader& operator >> (Poco::BinaryReader& reader, Poco::Net::SocketAddress& value)
{
	Poco::Net::IPAddress host;
	reader >> host;
	Poco::UInt16 port;
	reader >> port;
	value = Poco::Net::SocketAddress(host, port);
	return reader;
}


std::ostream& operator << (std::ostream& ostr, const Poco::Net::SocketAddress& address)
{
	ostr << address.toString();
	return ostr;
}
