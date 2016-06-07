
// Environment_VX.cpp
//
// $Id: //poco/1.4/Foundation/src/Environment_VX.cpp#3 $
//
// Library: Foundation
// Package: Core
// Module:  Environment
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Environment_VX.h"
#include "Poco/Exception.h"
#include "Poco/Buffer.h"
#include <VxWorks.h>
#include <envLib.h>
#include <hostLib.h>
#include <ifLib.h>
#include <sockLib.h>
#include <ioLib.h>
#include <version.h>
#include <cstring>
#include <unistd.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <netinet/if_ether.h>
#include <ifLib.h>
#include <unistd.h>


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
	_map[name] = var;
	if (putenv((char*) _map[name].c_str()))
	{
		std::string msg = "cannot set environment variable: ";
		msg.append(name);
		throw SystemException(msg);
	}
}


std::string EnvironmentImpl::osNameImpl()
{
	return runtimeName;
}


std::string EnvironmentImpl::osDisplayNameImpl()
{
	return osNameImpl();
}


std::string EnvironmentImpl::osVersionImpl()
{
	return runtimeVersion;
}


std::string EnvironmentImpl::osArchitectureImpl()
{
#if POCO_ARCH == POCO_ARCH_IA32
	return "i386";
#elif POCO_ARCH == POCO_ARCH_MIPS
	return "mips";
#elif POCO_ARCH == POCO_ARCH_PPC
	return "ppc";
#elif POCO_ARCH == POCO_ARCH_ARM
	return "arm";
#elif POCO_ARCH == POCO_ARCH_SH
	return "sh";
#else
	return "unknown";
#endif	
}


std::string EnvironmentImpl::nodeNameImpl()
{
	char buffer[64];
	if (gethostname(buffer, sizeof(buffer)) == OK)
		return buffer;
	else
		return "unknown";
}


unsigned EnvironmentImpl::processorCountImpl()
{
	return 1;
}


void EnvironmentImpl::nodeIdImpl(NodeId& id)
{
	std::memset(&id, 0, sizeof(id));

	int ifIndex = 1;
	char ifName[32];
	for (;;)
	{
		if (ifIndexToIfName(ifIndex, ifName) == OK)
		{
			struct ifnet* pIf = ifunit(ifName);
			if (pIf)
			{
				std::memcpy(&id, ((struct arpcom *) pIf)->ac_enaddr, sizeof(id));
				return;
			}
		}
		else break;	
		++ifIndex;
	}
	throw SystemException("cannot get Ethernet hardware address");
}


} // namespace Poco
