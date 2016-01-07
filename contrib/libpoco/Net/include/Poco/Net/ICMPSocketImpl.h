//
// ICMPSocketImpl.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/ICMPSocketImpl.h#1 $
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
	ICMPSocketImpl(IPAddress::Family family, int dataSize, int ttl, int timeout);
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

protected:
	~ICMPSocketImpl();

private:
	ICMPPacket _icmpPacket;
	int _timeout;
};


} } // namespace Poco::Net


#endif // Net_ICMPSocketImpl_INCLUDED
