//
// SocketAddressImpl.h
//
// Library: Net
// Package: NetCore
// Module:  SocketAddressImpl
//
// Definition of the SocketAddressImpl class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketAddressImpl_INCLUDED
#define Net_SocketAddressImpl_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketDefs.h"
#include "Poco/Net/IPAddress.h"
#include "Poco/RefCountedObject.h"


namespace Poco {
namespace Net {
namespace Impl {


class Net_API SocketAddressImpl : public Poco::RefCountedObject
{
public:
	typedef AddressFamily::Family Family;

	virtual ~SocketAddressImpl();

	virtual IPAddress host() const = 0;
	virtual UInt16 port() const = 0;
	virtual poco_socklen_t length() const = 0;
	virtual const struct sockaddr* addr() const = 0;
	virtual int af() const = 0;
	virtual Family family() const = 0;
	virtual std::string toString() const = 0;

protected:
	SocketAddressImpl();

private:
	SocketAddressImpl(const SocketAddressImpl&);
	SocketAddressImpl& operator = (const SocketAddressImpl&);
};


class Net_API IPv4SocketAddressImpl: public SocketAddressImpl
{
public:
	IPv4SocketAddressImpl();
	IPv4SocketAddressImpl(const struct sockaddr_in* addr);
	IPv4SocketAddressImpl(const void* addr, UInt16 port);
	IPAddress host() const;
	UInt16 port() const;
	poco_socklen_t length() const;
	const struct sockaddr* addr() const;
	int af() const;
	Family family() const;
	std::string toString() const;

private:
	struct sockaddr_in _addr;
};


//
// inlines
//

inline IPAddress IPv4SocketAddressImpl::host() const
{
	return IPAddress(&_addr.sin_addr, sizeof(_addr.sin_addr));
}


inline UInt16 IPv4SocketAddressImpl::port() const
{
	return _addr.sin_port;
}


inline poco_socklen_t IPv4SocketAddressImpl::length() const
{
	return sizeof(_addr);
}


inline const struct sockaddr* IPv4SocketAddressImpl::addr() const
{
	return reinterpret_cast<const struct sockaddr*>(&_addr);
}


inline int IPv4SocketAddressImpl::af() const
{
	return _addr.sin_family;
}


inline SocketAddressImpl::Family IPv4SocketAddressImpl::family() const
{
	return AddressFamily::IPv4;
}


#if defined(POCO_HAVE_IPv6)


class Net_API IPv6SocketAddressImpl: public SocketAddressImpl
{
public:
	IPv6SocketAddressImpl(const struct sockaddr_in6* addr);
	IPv6SocketAddressImpl(const void* addr, UInt16 port);
	IPv6SocketAddressImpl(const void* addr, UInt16 port, UInt32 scope);
	IPAddress host() const;
	UInt16 port() const;
	poco_socklen_t length() const;
	const struct sockaddr* addr() const;
	int af() const;
	Family family() const;
	std::string toString() const;

private:
	struct sockaddr_in6 _addr;
};


//
// inlines
//

inline IPAddress IPv6SocketAddressImpl::host() const
{
	return IPAddress(&_addr.sin6_addr, sizeof(_addr.sin6_addr), _addr.sin6_scope_id);
}


inline UInt16 IPv6SocketAddressImpl::port() const
{
	return _addr.sin6_port;
}


inline poco_socklen_t IPv6SocketAddressImpl::length() const
{
	return sizeof(_addr);
}


inline const struct sockaddr* IPv6SocketAddressImpl::addr() const
{
	return reinterpret_cast<const struct sockaddr*>(&_addr);
}


inline int IPv6SocketAddressImpl::af() const
{
	return _addr.sin6_family;
}


inline SocketAddressImpl::Family IPv6SocketAddressImpl::family() const
{
	return AddressFamily::IPv6;
}


#endif // POCO_HAVE_IPv6


#if defined(POCO_OS_FAMILY_UNIX)


class Net_API LocalSocketAddressImpl: public SocketAddressImpl
{
public:
	LocalSocketAddressImpl(const struct sockaddr_un* addr);
	LocalSocketAddressImpl(const char* path);
	LocalSocketAddressImpl(const char* path, std::size_t length);
	~LocalSocketAddressImpl();
	IPAddress host() const;
	UInt16 port() const;
	poco_socklen_t length() const;
	const struct sockaddr* addr() const;
	int af() const;
	Family family() const;
	const char* path() const;
	std::string toString() const;

private:
	struct sockaddr_un* _pAddr;
		// Note: We allocate struct sockaddr_un on the heap, otherwise we would
		// waste a lot of memory due to small object optimization in SocketAddress.
};


//
// inlines
//

inline IPAddress LocalSocketAddressImpl::host() const
{
	throw Poco::InvalidAccessException("local socket address does not have host IP address");
}


inline UInt16 LocalSocketAddressImpl::port() const
{
	throw Poco::InvalidAccessException("local socket address does not have port number");
}


inline poco_socklen_t LocalSocketAddressImpl::length() const
{
	return sizeof(struct sockaddr_un);
}


inline const struct sockaddr* LocalSocketAddressImpl::addr() const
{
	return reinterpret_cast<const struct sockaddr*>(_pAddr);
}


inline int LocalSocketAddressImpl::af() const
{
	return _pAddr->sun_family;
}


inline SocketAddressImpl::Family LocalSocketAddressImpl::family() const
{
	return AddressFamily::UNIX_LOCAL;
}


inline const char* LocalSocketAddressImpl::path() const
{
	return _pAddr->sun_path;
}


#endif // POCO_OS_FAMILY_UNIX


} } } // namespace Poco::Net::Impl


#endif // Net_SocketAddressImpl_INCLUDED
