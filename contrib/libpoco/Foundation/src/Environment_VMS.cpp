//
// Environment_VMS.cpp
//
// $Id: //poco/1.4/Foundation/src/Environment_VMS.cpp#2 $
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


#include "Poco/Environment_VMS.h"
#include "Poco/Exception.h"
#include <stdlib.h>
#include <starlet.h>
#include <descrip.h>
#include <ssdef.h>
#include <syidef.h>
#include <iledef.h>
#include <lnmdef.h>
#include <ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <net/if.h>
#include <inet.h>
#include <netdb.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <unistd.h>


#define MAXHOSTNAMELEN 64


namespace Poco {


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
	
	if (setenv(name.c_str(), value.c_str(), 1))
	{
		std::string msg = "cannot set environment variable: ";
		msg.append(name);
		throw SystemException(msg);
	}
}


std::string EnvironmentImpl::osNameImpl()
{
	return getsyi(SYI$_NODE_SWTYPE);
}


std::string EnvironmentImpl::osDisplayNameImpl()
{
	return osNameImpl();
}


std::string EnvironmentImpl::osVersionImpl()
{
	return getsyi(SYI$_VERSION);
}


std::string EnvironmentImpl::osArchitectureImpl()
{
	return getsyi(SYI$_ARCH_NAME);
}


std::string EnvironmentImpl::nodeNameImpl()
{
	return getsyi(SYI$_NODENAME);
}


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


unsigned EnvironmentImpl::processorCountImpl()
{
	#pragma pointer_size save
	#pragma pointer_size 32

	Poco::UInt32 count;
	unsigned short length;

	ILE3 items[2];
	items[0].ile3$w_code         = SYI$_ACTIVECPU_CNT;
	items[0].ile3$w_length       = sizeof(count);
	items[0].ile3$ps_bufaddr     = &count;
	items[0].ile3$ps_retlen_addr = &length;
	items[1].ile3$w_code         = 0;
	items[1].ile3$w_length       = 0;

	if (sys$getsyiw(0, 0, 0, items, 0, 0, 0) == 1)
		return count;
	else
		throw SystemException("$GETSYI failed");

	#pragma pointer_size restore
}


std::string EnvironmentImpl::getsyi(unsigned short code)
{
	#pragma pointer_size save
	#pragma pointer_size 32

	unsigned char result[16];
	unsigned short length;

	ILE3 items[2];
	items[0].ile3$w_code         = code;
	items[0].ile3$w_length       = sizeof(result);
	items[0].ile3$ps_bufaddr     = result;
	items[0].ile3$ps_retlen_addr = &length;
	items[1].ile3$w_code         = 0;
	items[1].ile3$w_length       = 0;

	if (sys$getsyiw(0, 0, 0, items, 0, 0, 0) == 1)
		return std::string((char*) result, length);
	else
		throw SystemException("$GETSYI failed");

	#pragma pointer_size restore
}


std::string EnvironmentImpl::trnlnm(const std::string& name)
{
	#pragma pointer_size save
	#pragma pointer_size 32

	unsigned char result[LNM$C_NAMLENGTH];
	unsigned short length;
	
	ILE3 items[2];
	items[0].ile3$w_code         = LNM$_STRING;
	items[0].ile3$w_length       = sizeof(result);
	items[0].ile3$ps_bufaddr     = result;
	items[0].ile3$ps_retlen_addr = &length;
	items[1].ile3$w_code         = 0;
	items[1].ile3$w_length       = 0;

	#pragma pointer_size restore

	unsigned int trnAttr = LNM$M_CASE_BLIND;
	POCO_DESCRIPTOR_LITERAL(tableDsc, "LNM$FILE_DEV");
	POCO_DESCRIPTOR_STRING(nameDsc, name);
	if (sys$trnlnm(&trnAttr, &tableDsc, &nameDsc, 0, &items) == 1)
	{
		if (result[0] == 0x1B)
			return std::string((char*) result + 4, length - 4);
		else
			return std::string((char*) result, length);
	}
	else
	{
		return std::string();
	}
}


} // namespace Poco
