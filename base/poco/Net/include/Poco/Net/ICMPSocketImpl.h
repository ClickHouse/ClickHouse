//
// ICMPSocketImpl.h
//
// Library: Net
// Package: ICMP
// Module:  ICMPSocketImpl
//
// Definition of the ICMPSocketImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_ICMPSocketImpl_INCLUDED
#define Net_ICMPSocketImpl_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/RawSocketImpl.h"
#include "Poco/Net/ICMPPacket.h"
#include "Poco/Timestamp.h"


namespace Poco {
namespace Net {


class Net_API ICMPSocketImpl: public RawSocketImpl
	/// This class implements an ICMP socket.
{
public:
	ICMPSocketImpl(SocketAddress::Family family, int dataSize, int ttl, int timeout);
		/// Creates an unconnected ICMP socket.
		///
		/// The socket will be created for the given address family.

	int sendTo(const void*, int, const SocketAddress& address, int flags = 0);
		/// Sends an ICMP request through the socket to the given address.
		///
		/// Returns the number of bytes sent.
	
	int receiveFrom(void*, int, SocketAddress& address, int flags = 0);
		/// Receives data from the socket.
		/// Stores the address of the sender in address.
		///
		/// Returns the time elapsed since the originating request was sent.

	int dataSize() const;
		/// Returns the data size in bytes.

	int ttl() const;
		/// Returns the Time-To-Live value.

	int timeout() const;
		/// Returns the socket timeout value.

protected:
	~ICMPSocketImpl();

private:
	ICMPPacket _icmpPacket;
	int _ttl;
	int _timeout;
};


//
// inlines
//
inline int ICMPSocketImpl::dataSize() const
{
	return _icmpPacket.getDataSize();
}


inline int ICMPSocketImpl::ttl() const
{
	return _ttl;
}


inline int ICMPSocketImpl::timeout() const
{
	return _timeout;
}


} } // namespace Poco::Net


#endif // Net_ICMPSocketImpl_INCLUDED
