//
// DNS.cpp
//
// $Id: //poco/1.4/Net/src/DNS.cpp#10 $
//
// Library: Net
// Package: NetCore
// Module:  DNS
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/DNS.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Environment.h"
#include "Poco/NumberFormatter.h"
#include "Poco/RWLock.h"
#include <cstring>


#if defined(POCO_HAVE_LIBRESOLV)
#include <resolv.h>
#endif


using Poco::Environment;
using Poco::NumberFormatter;
using Poco::IOException;


namespace Poco {
namespace Net {


#if defined(POCO_HAVE_LIBRESOLV)
static Poco::RWLock resolverLock;
#endif


HostEntry DNS::hostByName(const std::string& hostname, unsigned 
#ifdef POCO_HAVE_ADDRINFO
						  hintFlags
#endif
						 )
{
#if defined(POCO_HAVE_LIBRESOLV)
	Poco::ScopedReadRWLock readLock(resolverLock);
#endif

#if defined(POCO_HAVE_ADDRINFO)
	struct addrinfo* pAI;
	struct addrinfo hints;
	std::memset(&hints, 0, sizeof(hints));
	hints.ai_flags = hintFlags;
	int rc = getaddrinfo(hostname.c_str(), NULL, &hints, &pAI); 
	if (rc == 0)
	{
		HostEntry result(pAI);
		freeaddrinfo(pAI);
		return result;
	}
	else
	{
		aierror(rc, hostname);
	}
#elif defined(POCO_VXWORKS)
	int addr = hostGetByName(const_cast<char*>(hostname.c_str()));
	if (addr != ERROR)
	{
		return HostEntry(hostname, IPAddress(&addr, sizeof(addr)));
	}
#else
	struct hostent* he = gethostbyname(hostname.c_str());
	if (he)
	{
		return HostEntry(he);
	}
#endif
	error(lastError(), hostname); // will throw an appropriate exception
	throw NetException(); // to silence compiler
}


HostEntry DNS::hostByAddress(const IPAddress& address, unsigned 
#ifdef POCO_HAVE_ADDRINFO
							 hintFlags
#endif
							)
{
#if defined(POCO_HAVE_LIBRESOLV)
	Poco::ScopedReadRWLock readLock(resolverLock);
#endif

#if defined(POCO_HAVE_ADDRINFO)
	SocketAddress sa(address, 0);
	static char fqname[1024];
	int rc = getnameinfo(sa.addr(), sa.length(), fqname, sizeof(fqname), NULL, 0, NI_NAMEREQD); 
	if (rc == 0)
	{
		struct addrinfo* pAI;
		struct addrinfo hints;
		std::memset(&hints, 0, sizeof(hints));
		hints.ai_flags = hintFlags;
		rc = getaddrinfo(fqname, NULL, &hints, &pAI);
		if (rc == 0)
		{
			HostEntry result(pAI);
			freeaddrinfo(pAI);
			return result;
		}
		else
		{
			aierror(rc, address.toString());
		}
	}
	else
	{
		aierror(rc, address.toString());
	}
#elif defined(POCO_VXWORKS)
	char name[MAXHOSTNAMELEN + 1];
	if (hostGetByAddr(*reinterpret_cast<const int*>(address.addr()), name) == OK)
	{
		return HostEntry(std::string(name), address);
	}
#else
	struct hostent* he = gethostbyaddr(reinterpret_cast<const char*>(address.addr()), address.length(), address.af());
	if (he)
	{
		return HostEntry(he);
	}
#endif
	int err = lastError();
	error(err, address.toString()); // will throw an appropriate exception
	throw NetException(); // to silence compiler
}


HostEntry DNS::resolve(const std::string& address)
{
	IPAddress ip;
	if (IPAddress::tryParse(address, ip))
		return hostByAddress(ip);
	else
		return hostByName(address);
}


IPAddress DNS::resolveOne(const std::string& address)
{
	const HostEntry& entry = resolve(address);
	if (!entry.addresses().empty())
		return entry.addresses()[0];
	else
		throw NoAddressFoundException(address);
}


HostEntry DNS::thisHost()
{
	return hostByName(hostName());
}


void DNS::reload()
{
#if defined(POCO_HAVE_LIBRESOLV)
	Poco::ScopedWriteRWLock writeLock(resolverLock);
	res_init();
#endif
}


void DNS::flushCache()
{
}


std::string DNS::hostName()
{
	char buffer[256];
	int rc = gethostname(buffer, sizeof(buffer));
	if (rc == 0)
		return std::string(buffer);
	else
		throw NetException("Cannot get host name");
}


int DNS::lastError()
{
#if defined(_WIN32)
	return GetLastError();
#elif defined(POCO_VXWORKS)
	return errno;
#else
	return h_errno;
#endif
}

	
void DNS::error(int code, const std::string& arg)
{
	switch (code)
	{
	case POCO_ESYSNOTREADY:
		throw NetException("Net subsystem not ready");
	case POCO_ENOTINIT:
		throw NetException("Net subsystem not initialized");
	case POCO_HOST_NOT_FOUND:
		throw HostNotFoundException(arg);
	case POCO_TRY_AGAIN:
		throw DNSException("Temporary DNS error while resolving", arg);
	case POCO_NO_RECOVERY:
		throw DNSException("Non recoverable DNS error while resolving", arg);
	case POCO_NO_DATA:
		throw NoAddressFoundException(arg);
	default:
		throw IOException(NumberFormatter::format(code));
	}
}


void DNS::aierror(int code, const std::string& arg)
{
#if defined(POCO_HAVE_IPv6) || defined(POCO_HAVE_ADDRINFO)
	switch (code)
	{
	case EAI_AGAIN:
		throw DNSException("Temporary DNS error while resolving", arg);
	case EAI_FAIL:
		throw DNSException("Non recoverable DNS error while resolving", arg);
#if !defined(_WIN32) // EAI_NODATA and EAI_NONAME have the same value
#if defined(EAI_NODATA) // deprecated in favor of EAI_NONAME on FreeBSD
	case EAI_NODATA:
		throw NoAddressFoundException(arg);
#endif
#endif
	case EAI_NONAME:
		throw HostNotFoundException(arg);
#if defined(EAI_SYSTEM)
	case EAI_SYSTEM:
		error(lastError(), arg);
		break;
#endif
#if defined(_WIN32)
	case WSANO_DATA: // may happen on XP
		throw HostNotFoundException(arg);
#endif
	default:
		throw DNSException("EAI", NumberFormatter::format(code));
	}
#endif // POCO_HAVE_IPv6 || defined(POCO_HAVE_ADDRINFO)
}


} } // namespace Poco::Net
