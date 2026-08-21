//
// SocketAddressImpl.cpp
//
// Library: Net
// Package: NetCore
// Module:  SocketAddressImpl
//
// Copyright (c) 2005-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketAddressImpl.h"
#include "Poco/Net/SocketDefs.h"
#include "Poco/NumberFormatter.h"
#include <cstring>


namespace Poco {
namespace Net {
namespace Impl {


//
// SocketAddressImpl
//


SocketAddressImpl::SocketAddressImpl()
{
}


SocketAddressImpl::~SocketAddressImpl()
{
}


//
// IPv4SocketAddressImpl
//


IPv4SocketAddressImpl::IPv4SocketAddressImpl()
{
	std::memset(&_addr, 0, sizeof(_addr));
	_addr.sin_family = AF_INET;
	poco_set_sin_len(&_addr);
}


IPv4SocketAddressImpl::IPv4SocketAddressImpl(const struct sockaddr_in* addr)
{
	std::memcpy(&_addr, addr, sizeof(_addr));
}


IPv4SocketAddressImpl::IPv4SocketAddressImpl(const void* addr, UInt16 port)
{
	std::memset(&_addr, 0, sizeof(_addr));
	_addr.sin_family = AF_INET;
	poco_set_sin_len(&_addr);
	std::memcpy(&_addr.sin_addr, addr, sizeof(_addr.sin_addr));
	_addr.sin_port = port;
}


std::string IPv4SocketAddressImpl::toString() const
{
	std::string result;
	result.append(host().toString());
	result.append(":");
	NumberFormatter::append(result, ntohs(port()));
	return result;
}


#if defined(POCO_HAVE_IPv6)


//
// IPv6SocketAddressImpl
//


IPv6SocketAddressImpl::IPv6SocketAddressImpl(const struct sockaddr_in6* addr)
{
	std::memcpy(&_addr, addr, sizeof(_addr));
}


IPv6SocketAddressImpl::IPv6SocketAddressImpl(const void* addr, UInt16 port)
{
	std::memset(&_addr, 0, sizeof(_addr));
	_addr.sin6_family = AF_INET6;
	poco_set_sin6_len(&_addr);
	std::memcpy(&_addr.sin6_addr, addr, sizeof(_addr.sin6_addr));
	_addr.sin6_port = port;
}


IPv6SocketAddressImpl::IPv6SocketAddressImpl(const void* addr, UInt16 port, UInt32 scope)
{
	std::memset(&_addr, 0, sizeof(_addr));
	_addr.sin6_family = AF_INET6;
	poco_set_sin6_len(&_addr);
	std::memcpy(&_addr.sin6_addr, addr, sizeof(_addr.sin6_addr));
	_addr.sin6_port = port;
	_addr.sin6_scope_id = scope;
}


std::string IPv6SocketAddressImpl::toString() const
{
	std::string result;
	result.append("[");
	result.append(host().toString());
	result.append("]");
	result.append(":");
	NumberFormatter::append(result, ntohs(port()));
	return result;
}


#endif // POCO_HAVE_IPv6


#if defined(POCO_OS_FAMILY_UNIX)


//
// LocalSocketAddressImpl
//


LocalSocketAddressImpl::LocalSocketAddressImpl(const struct sockaddr_un* addr)
{
	_pAddr = new sockaddr_un;
	std::memcpy(_pAddr, addr, sizeof(struct sockaddr_un));
}


LocalSocketAddressImpl::LocalSocketAddressImpl(const char* path)
{
	poco_assert (std::strlen(path) < sizeof(_pAddr->sun_path));

	_pAddr = new sockaddr_un;
	poco_set_sun_len(_pAddr, std::strlen(path) + sizeof(struct sockaddr_un) - sizeof(_pAddr->sun_path) + 1);
	_pAddr->sun_family = AF_UNIX;
	std::strcpy(_pAddr->sun_path, path);
}


LocalSocketAddressImpl::LocalSocketAddressImpl(const char* path, std::size_t length)
{
	poco_assert (length < sizeof(_pAddr->sun_path));

	_pAddr = new sockaddr_un;
	poco_set_sun_len(_pAddr, length + sizeof(struct sockaddr_un) - sizeof(_pAddr->sun_path) + 1);
	_pAddr->sun_family = AF_UNIX;
	std::memcpy(_pAddr->sun_path, path, length);
	_pAddr->sun_path[length] = 0;
}


LocalSocketAddressImpl::~LocalSocketAddressImpl()
{
	delete _pAddr;
}


std::string LocalSocketAddressImpl::toString() const
{
	std::string result(path());
	return result;
}


#endif // POCO_OS_FAMILY_UNIX


} } } // namespace Poco::Net::Impl
