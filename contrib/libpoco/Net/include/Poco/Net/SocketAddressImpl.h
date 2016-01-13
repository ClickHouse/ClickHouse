//
// SocketAddressImpl.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/SocketAddressImpl.h#2 $
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
#ifndef POCO_HAVE_ALIGNMENT
#include "Poco/RefCountedObject.h"
#endif

namespace Poco {
namespace Net {
namespace Impl {


class Net_API SocketAddressImpl
#ifndef POCO_HAVE_ALIGNMENT
	: public Poco::RefCountedObject
#endif
{
public:
	virtual ~SocketAddressImpl();

	virtual IPAddress host() const = 0;
	virtual UInt16 port() const = 0;
	virtual poco_socklen_t length() const = 0;
	virtual const struct sockaddr* addr() const = 0;
	virtual int af() const = 0;

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


#endif //POCO_HAVE_IPv6


} } } // namespace Poco::Net::Impl


#endif // Net_SocketAddressImpl_INCLUDED
