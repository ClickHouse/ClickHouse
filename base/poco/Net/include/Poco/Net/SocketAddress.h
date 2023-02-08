//
// SocketAddress.h
//
// Library: Net
// Package: NetCore
// Module:  SocketAddress
//
// Definition of the SocketAddress class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketAddress_INCLUDED
#define Net_SocketAddress_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketAddressImpl.h"
#include <ostream>


namespace Poco {

class BinaryReader;
class BinaryWriter;

namespace Net {


class IPAddress;


class Net_API SocketAddress
	/// This class represents an internet (IP) endpoint/socket
	/// address. The address can belong either to the
	/// IPv4 or the IPv6 address family and consists of a
	/// host address and a port number.
{
public:
	// The following declarations keep the Family type
	// backwards compatible with the previously used
	// enum declaration.
	typedef AddressFamily::Family Family;
	static const Family IPv4 = AddressFamily::IPv4;
#if defined(POCO_HAVE_IPv6)
	static const Family IPv6 = AddressFamily::IPv6;
#endif
#if defined(POCO_OS_FAMILY_UNIX)
	static const Family UNIX_LOCAL = AddressFamily::UNIX_LOCAL;
#endif

	SocketAddress();
		/// Creates a wildcard (all zero) IPv4 SocketAddress.

	explicit SocketAddress(Family family);
		/// Creates a SocketAddress with unspecified (wildcard) IP address
		/// of the given family.

	SocketAddress(const IPAddress& hostAddress, Poco::UInt16 portNumber);
		/// Creates a SocketAddress from an IP address and given port number.

	explicit SocketAddress(Poco::UInt16 port);
		/// Creates a SocketAddress with unspecified (wildcard) IP address
		/// and given port number.

	SocketAddress(Family family, Poco::UInt16 port);
		/// Creates a SocketAddress with unspecified (wildcard) IP address
		/// of the given family, and given port number.

	SocketAddress(const std::string& hostAddress, Poco::UInt16 portNumber);
		/// Creates a SocketAddress from an IP address and given port number.
		///
		/// The IP address must either be a domain name, or it must
		/// be in dotted decimal (IPv4) or hex string (IPv6) format.

	SocketAddress(Family family, const std::string& hostAddress, Poco::UInt16 portNumber);
		/// Creates a SocketAddress from an IP address and given port number.
		///
		/// The IP address must either be a domain name, or it must
		/// be in dotted decimal (IPv4) or hex string (IPv6) format.
		///
		/// If a domain name is given in hostAddress, it is resolved and the address
		/// matching the given family is used. If no address matching the given family
		/// is found, or the IP address given in hostAddress does not match the given
		/// family, an AddressFamilyMismatchException is thrown.

	SocketAddress(const std::string& hostAddress, const std::string& portNumber);
		/// Creates a SocketAddress from an IP address and the
		/// service name or port number.
		///
		/// The IP address must either be a domain name, or it must
		/// be in dotted decimal (IPv4) or hex string (IPv6) format.
		///
		/// The given port must either be a decimal port number, or
		/// a service name.

	SocketAddress(Family family, const std::string& hostAddress, const std::string& portNumber);
		/// Creates a SocketAddress from an IP address and the
		/// service name or port number.
		///
		/// The IP address must either be a domain name, or it must
		/// be in dotted decimal (IPv4) or hex string (IPv6) format.
		///
		/// The given port must either be a decimal port number, or
		/// a service name.
		///
		/// If a domain name is given in hostAddress, it is resolved and the address
		/// matching the given family is used. If no address matching the given family
		/// is found, or the IP address given in hostAddress does not match the given
		/// family, an AddressFamilyMismatchException is thrown.

	explicit SocketAddress(const std::string& hostAndPort);
		/// Creates a SocketAddress from an IP address or host name and the
		/// port number/service name. Host name/address and port number must
		/// be separated by a colon. In case of an IPv6 address,
		/// the address part must be enclosed in brackets.
		///
		/// Examples:
		///     192.168.1.10:80
		///     [::ffff:192.168.1.120]:2040
		///     www.appinf.com:8080
		///
		/// On POSIX platforms supporting UNIX_LOCAL sockets, hostAndPort
		/// can also be the absolute path of a local socket, starting with a
		/// slash, e.g. "/tmp/local.socket".

	SocketAddress(Family family, const std::string& addr);
		/// Creates a SocketAddress of the given family from a
		/// string representation of the address, which is
		/// either an IP address and port number, separated by
		/// a colon for IPv4 or IPv6 addresses, or a path for
		/// UNIX_LOCAL sockets.

	SocketAddress(const SocketAddress& addr);
		/// Creates a SocketAddress by copying another one.

	SocketAddress(const struct sockaddr* addr, poco_socklen_t length);
		/// Creates a SocketAddress from a native socket address.

	~SocketAddress();
		/// Destroys the SocketAddress.

	SocketAddress& operator = (const SocketAddress& socketAddress);
		/// Assigns another SocketAddress.

	IPAddress host() const;
		/// Returns the host IP address.

	Poco::UInt16 port() const;
		/// Returns the port number.

	poco_socklen_t length() const;
		/// Returns the length of the internal native socket address.

	const struct sockaddr* addr() const;
		/// Returns a pointer to the internal native socket address.

	int af() const;
		/// Returns the address family (AF_INET or AF_INET6) of the address.

	std::string toString() const;
		/// Returns a string representation of the address.

	Family family() const;
		/// Returns the address family of the host's address.

	bool operator < (const SocketAddress& socketAddress) const;
	bool operator == (const SocketAddress& socketAddress) const;
	bool operator != (const SocketAddress& socketAddress) const;

	enum
	{
		MAX_ADDRESS_LENGTH =
#if defined(POCO_OS_FAMILY_UNIX)
			sizeof(struct sockaddr_un)
#elif defined(POCO_HAVE_IPv6)
			sizeof(struct sockaddr_in6)
#else
			sizeof(struct sockaddr_in)
#endif
			/// Maximum length in bytes of a socket address.
	};

protected:
	void init(const IPAddress& hostAddress, Poco::UInt16 portNumber);
	void init(const std::string& hostAddress, Poco::UInt16 portNumber);
	void init(Family family, const std::string& hostAddress, Poco::UInt16 portNumber);
	void init(Family family, const std::string& address);
	void init(const std::string& hostAndPort);
	Poco::UInt16 resolveService(const std::string& service);

private:
	typedef Poco::Net::Impl::SocketAddressImpl Impl;
	typedef Poco::AutoPtr<Impl> Ptr;

	Ptr pImpl() const;

	void newIPv4();
	void newIPv4(const sockaddr_in*);
	void newIPv4(const IPAddress& hostAddress, Poco::UInt16 portNumber);

#if defined(POCO_HAVE_IPv6)
	void newIPv6(const sockaddr_in6*);
	void newIPv6(const IPAddress& hostAddress, Poco::UInt16 portNumber);
#endif

#if defined(POCO_OS_FAMILY_UNIX)
	void newLocal(const sockaddr_un* sockAddr);
	void newLocal(const std::string& path);
#endif

	Ptr _pImpl;
};


//
// inlines
//
inline SocketAddress::Ptr SocketAddress::pImpl() const
{
	if (_pImpl) return _pImpl;
	throw Poco::NullPointerException("Pointer to SocketAddress implementation is NULL.");
}


inline void SocketAddress::newIPv4()
{
	_pImpl = new Poco::Net::Impl::IPv4SocketAddressImpl;
}


inline void SocketAddress::newIPv4(const sockaddr_in* sockAddr)
{
	_pImpl = new Poco::Net::Impl::IPv4SocketAddressImpl(sockAddr);
}


inline void SocketAddress::newIPv4(const IPAddress& hostAddress, Poco::UInt16 portNumber)
{
	_pImpl = new Poco::Net::Impl::IPv4SocketAddressImpl(hostAddress.addr(), htons(portNumber));
}


#if defined(POCO_HAVE_IPv6)
inline void SocketAddress::newIPv6(const sockaddr_in6* sockAddr)
{
	_pImpl = new Poco::Net::Impl::IPv6SocketAddressImpl(sockAddr);
}


inline void SocketAddress::newIPv6(const IPAddress& hostAddress, Poco::UInt16 portNumber)
{
	_pImpl = new Poco::Net::Impl::IPv6SocketAddressImpl(hostAddress.addr(), htons(portNumber), hostAddress.scope());
}
#endif // POCO_HAVE_IPv6


#if defined(POCO_OS_FAMILY_UNIX)
inline void SocketAddress::newLocal(const sockaddr_un* sockAddr)
{
	_pImpl = new Poco::Net::Impl::LocalSocketAddressImpl(sockAddr);
}


inline void SocketAddress::newLocal(const std::string& path)
{
	_pImpl = new Poco::Net::Impl::LocalSocketAddressImpl(path.c_str(), path.size());
}
#endif // POCO_OS_FAMILY_UNIX


inline 	bool SocketAddress::operator == (const SocketAddress& socketAddress) const
{
#if defined(POCO_OS_FAMILY_UNIX)
	if (family() == UNIX_LOCAL)
		return toString() == socketAddress.toString();
	else
#endif
		return host() == socketAddress.host() && port() == socketAddress.port();
}


inline bool SocketAddress::operator != (const SocketAddress& socketAddress) const
{
	return !(operator == (socketAddress));
}


} } // namespace Poco::Net


Net_API Poco::BinaryWriter& operator << (Poco::BinaryWriter& writer, const Poco::Net::SocketAddress& value);
Net_API Poco::BinaryReader& operator >> (Poco::BinaryReader& reader, Poco::Net::SocketAddress& value);
Net_API std::ostream& operator << (std::ostream& ostr, const Poco::Net::SocketAddress& address);


#endif // Net_SocketAddress_INCLUDED
