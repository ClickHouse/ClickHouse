//
// MulticastSocket.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/MulticastSocket.h#1 $
//
// Library: Net
// Package: Sockets
// Module:  MulticastSocket
//
// Definition of the MulticastSocket class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_MulticastSocket_INCLUDED
#define Net_MulticastSocket_INCLUDED


#include "Poco/Net/Net.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "Poco/Net/DatagramSocket.h"
#include "Poco/Net/NetworkInterface.h"


namespace Poco {
namespace Net {


class Net_API MulticastSocket: public DatagramSocket
	/// A MulticastSocket is a special DatagramSocket
	/// that can be used to send packets to and receive
	/// packets from multicast groups.
{
public:
	MulticastSocket();
		/// Creates the MulticastSocket.

	explicit MulticastSocket(IPAddress::Family family);
		/// Creates an unconnected datagram socket.
		///
		/// The socket will be created for the
		/// given address family.

	MulticastSocket(const SocketAddress& address, bool reuseAddress = false);
		/// Creates a datagram socket and binds it
		/// to the given address.
		///
		/// Depending on the address family, the socket
		/// will be either an IPv4 or an IPv6 socket.

	MulticastSocket(const Socket& socket);
		/// Creates the DatagramSocket with the SocketImpl
		/// from another socket. The SocketImpl must be
		/// a DatagramSocketImpl, otherwise an InvalidArgumentException
		/// will be thrown.

	~MulticastSocket();
		/// Destroys the DatagramSocket.

	MulticastSocket& operator = (const Socket& socket);
		/// Assignment operator.
		///
		/// Releases the socket's SocketImpl and
		/// attaches the SocketImpl from the other socket and
		/// increments the reference count of the SocketImpl.

	void setInterface(const NetworkInterface& interfc);
		/// Sets the interface used for sending multicast packets.
		///
		/// To select the default interface, specify an empty
		/// interface.
		///
		/// This is done by setting the IP_MULTICAST_IF/IPV6_MULTICAST_IF
		/// socket option.

	NetworkInterface getInterface() const;
		/// Returns the interface used for sending multicast packets.

	void setLoopback(bool flag);
		/// Enable or disable loopback for multicast packets.
		///
		/// Sets the value of the IP_MULTICAST_LOOP/IPV6_MULTICAST_LOOP
		/// socket option.

	bool getLoopback() const;
		/// Returns true iff loopback for multicast packets is enabled,
		/// false otherwise.

	void setTimeToLive(unsigned value);
		/// Specifies the TTL/hop limit for outgoing packets.
		///
		/// Sets the value of the IP_MULTICAST_TTL/IPV6_MULTICAST_HOPS
		/// socket option.

	unsigned getTimeToLive() const;
		/// Returns the TTL/hop limit for outgoing packets.
		
	void joinGroup(const IPAddress& groupAddress);
		/// Joins the specified multicast group at the default interface.

	void joinGroup(const IPAddress& groupAddress, const NetworkInterface& interfc);
		/// Joins the specified multicast group at the given interface.

	void leaveGroup(const IPAddress& groupAddress);
		/// Leaves the specified multicast group at the default interface.

	void leaveGroup(const IPAddress& groupAddress, const NetworkInterface& interfc);
		/// Leaves the specified multicast group at the given interface.

private:
	static NetworkInterface findFirstInterface(const IPAddress& groupAddress);
		/// Returns first multicast-eligible network interface.
};


} } // namespace Poco::Net


#endif // POCO_NET_HAS_INTERFACE


#endif // Net_MulticastSocket_INCLUDED
