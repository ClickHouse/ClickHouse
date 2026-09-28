
// Environment_UNIX.cpp
//
// Library: Foundation
// Package: Core
// Module:  Environment
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Environment_UNIX.h"
#include "Poco/Exception.h"
#include "Poco/Buffer.h"
#include <cstring>
#include <unistd.h>
#include <stdlib.h>
#include <sys/utsname.h>
#include <sys/param.h>
#if defined(POCO_OS_FAMILY_BSD)
#include <sys/sysctl.h>
#elif POCO_OS == POCO_OS_HPUX
#include <pthread.h>
#endif

#include <sys/types.h>
#include <sys/socket.h>

namespace Poco {


EnvironmentImpl::StringMap EnvironmentImpl::_map;
FastMutex EnvironmentImpl::_mutex;


std::string EnvironmentImpl::getImpl(const std::string& name)
{
	FastMutex::ScopedLock lock(_mutex);

	const char* val = getenv(name.c_str());
	if (val)
		return std::string(val);
	else
		throw NotFoundException(name);
}


bool EnvironmentImpl::hasImpl(const std::string& name)
{
	FastMutex::ScopedLock lock(_mutex);

	return getenv(name.c_str()) != 0;
}


void EnvironmentImpl::setImpl(const std::string& name, const std::string& value)
{
	FastMutex::ScopedLock lock(_mutex);

	std::string var = name;
	var.append("=");
	var.append(value);
	std::swap(_map[name], var);
	if (putenv((char*) _map[name].c_str()))
	{
		std::string msg = "cannot set environment variable: ";
		msg.append(name);
		throw SystemException(msg);
	}
}


std::string EnvironmentImpl::osNameImpl()
{
	struct utsname uts;
	uname(&uts);
	return uts.sysname;
}


std::string EnvironmentImpl::osDisplayNameImpl()
{
	return osNameImpl();
}


std::string EnvironmentImpl::osVersionImpl()
{
	struct utsname uts;
	uname(&uts);
	return uts.release;
}


std::string EnvironmentImpl::osArchitectureImpl()
{
	struct utsname uts;
	uname(&uts);
	return uts.machine;
}


std::string EnvironmentImpl::nodeNameImpl()
{
	struct utsname uts;
	uname(&uts);
	return uts.nodename;
}


unsigned EnvironmentImpl::processorCountImpl()
{
#if defined(_SC_NPROCESSORS_ONLN)
	int count = sysconf(_SC_NPROCESSORS_ONLN);
	if (count <= 0) count = 1;
	return static_cast<int>(count);
#elif defined(POCO_OS_FAMILY_BSD)
	unsigned count;
	std::size_t size = sizeof(count);
	if (sysctlbyname("hw.ncpu", &count, &size, 0, 0))
		return 1;
	else
		return count;
#elif POCO_OS == POCO_OS_HPUX
	return pthread_num_processors_np();
#else
	return 1;
#endif
}


} // namespace Poco


//
// nodeIdImpl
//
#if defined(POCO_OS_FAMILY_BSD) || POCO_OS == POCO_OS_QNX
//
// BSD variants
//
#include <ifaddrs.h>
#include <net/if_dl.h>


namespace Poco {


void EnvironmentImpl::nodeIdImpl(NodeId& id)
{
	std::memset(&id, 0, sizeof(id));
	struct ifaddrs* ifaphead;
	int rc = getifaddrs(&ifaphead);
	if (rc) return;

	for (struct ifaddrs* ifap = ifaphead; ifap; ifap = ifap->ifa_next)
	{
		if (ifap->ifa_addr && ifap->ifa_addr->sa_family == AF_LINK)
		{
			struct sockaddr_dl* sdl = reinterpret_cast<struct sockaddr_dl*>(ifap->ifa_addr);
			caddr_t ap = LLADDR(sdl);
			int alen = sdl->sdl_alen;
			if (ap && alen > 0)
			{
				std::memcpy(&id, ap, sizeof(id));
				break;
			}
		}
	}
	freeifaddrs(ifaphead);
}


} // namespace Poco


#elif defined(__CYGWIN__) || POCO_OS == POCO_OS_LINUX || POCO_OS == POCO_OS_ANDROID
//
// Linux, Cygwin
//
#include <sys/ioctl.h>
/// #include <sys/socket.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <arpa/inet.h>
/// #include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdio>


namespace Poco {


void EnvironmentImpl::nodeIdImpl(NodeId& id)
{
	std::memset(&id, 0, sizeof(id));

	// ideally, the following code should be rewritten
	// to use netlink

	// first try to obtain the MAC address of eth0 using /sys/class/net
	int fd = open("/sys/class/net/eth0/address", O_RDONLY);
	if (fd >= 0)
	{
		char buffer[18];
		int n = read(fd, buffer, 17);
		close(fd);
		if (n == 17)
		{
			buffer[n] = 0;
			if (std::sscanf(buffer, "%hhx:%hhx:%hhx:%hhx:%hhx:%hhx", &id[0], &id[1], &id[2], &id[3], &id[4], &id[5]) == 6)
				return;
		}
	}

	// if that did not work, search active interfaces
	int sock = socket(PF_INET, SOCK_DGRAM, 0);
	if (sock == -1) return;

	// the following code is loosely based
	// on W. Richard Stevens, UNIX Network Programming, pp 434ff.
	int lastlen = 0;
	int len = 100*sizeof(struct ifreq);
	struct ifconf ifc;
	char* buf = 0;
	for (;;)
	{
		buf = new char[len];
		ifc.ifc_len = len;
		ifc.ifc_buf = buf;
		if (::ioctl(sock, SIOCGIFCONF, &ifc) < 0)
		{
			if (errno != EINVAL || lastlen != 0)
			{
				close(sock);
				delete [] buf;
				return;
			}
		}
		else
		{
			if (ifc.ifc_len == lastlen)
				break;
			lastlen = ifc.ifc_len;
		}
		len += 10*sizeof(struct ifreq);
		delete [] buf;
	}
	for (const char* ptr = buf; ptr < buf + ifc.ifc_len;)
	{
		const struct ifreq* ifr = reinterpret_cast<const struct ifreq*>(ptr);
		int rc = ioctl(sock, SIOCGIFHWADDR, ifr);
		if (rc != -1)
		{
			const struct sockaddr* sa = reinterpret_cast<const struct sockaddr*>(&ifr->ifr_hwaddr);
			if (sa->sa_family == ARPHRD_ETHER)
			{
				std::memcpy(&id, sa->sa_data, sizeof(id));
				break;
			}
		}
		ptr += sizeof(struct ifreq);
	}
	close(sock);
	delete [] buf;
}


} // namespace Poco


#elif defined(POCO_OS_FAMILY_UNIX)
//
// General Unix
//
/// #include <sys/ioctl.h>
#if defined(sun) || defined(__sun)
#include <sys/sockio.h>
#include <netdb.h>
#include <net/if.h>
#include <net/if_arp.h>
#endif
/// #include <sys/socket.h>
/// #include <sys/types.h>
/// #include <netinet/in.h>
/// #include <net/if.h>
/// #include <arpa/inet.h>
/// #include <unistd.h>


namespace Poco {


void EnvironmentImpl::nodeIdImpl(NodeId& id)
{
	std::memset(&id, 0, sizeof(id));
	char name[MAXHOSTNAMELEN];
	if (gethostname(name, sizeof(name)))
		return;

	struct hostent* pHost = gethostbyname(name);
	if (!pHost) return;

	int s = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (s == -1) return;

	struct arpreq ar;
	std::memset(&ar, 0, sizeof(ar));
	struct sockaddr_in* pAddr = reinterpret_cast<struct sockaddr_in*>(&ar.arp_pa);
	pAddr->sin_family = AF_INET;
	std::memcpy(&pAddr->sin_addr, *pHost->h_addr_list, sizeof(struct in_addr));
	int rc = ioctl(s, SIOCGARP, &ar);
	close(s);
	if (rc < 0) return;
	std::memcpy(&id, ar.arp_ha.sa_data, sizeof(id));
}


} // namespace Poco


#endif
