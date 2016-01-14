//
// DatagramSocket.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/DatagramSocket.h#1 $
//
// Library: Net
// Package: Sockets
// Module:  DatagramSocket
//
// Definition of the DatagramSocket class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_DatagramSocket_INCLUDED
#define Net_DatagramSocket_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/Socket.h"


namespace Poco {
namespace Net {


class Net_API DatagramSocket: public Socket
	/// This class provides an interface to an
	/// UDP stream socket.
{
public:
	DatagramSocket();
		/// Creates an unconnected IPv4 datagram socket.

	explicit DatagramSocket(IPAddress::Family family);
		/// Creates an unconnected datagram socket.
		///
		/// The socket will be created for the
		/// given address family.

	DatagramSocket(const SocketAddress& address, bool reuseAddress = false);
		/// Creates a datagram socket and binds it
		/// to the given address.
		///
		/// Depending on the address family, the socket
		/// will be either an IPv4 or an IPv6 socket.

	DatagramSocket(const Socket& socket);
		/// Creates the DatagramSocket with the SocketImpl
		/// from another socket. The SocketImpl must be
		/// a DatagramSocketImpl, otherwise an InvalidArgumentException
		/// will be thrown.

	~DatagramSocket();
		/// Destroys the DatagramSocket.

	DatagramSocket& operator = (const Socket& socket);
		/// Assignment operator.
		///
		/// Releases the socket's SocketImpl and
		/// attaches the SocketImpl from the other socket and
		/// increments the reference count of the SocketImpl.	

	void connect(const SocketAddress& address);
		/// Restricts incoming and outgoing
		/// packets to the specified address.
		///
		/// Calls to connect() cannot come before calls to bind().

	void bind(const SocketAddress& address, bool reuseAddress = false);
		/// Bind a local address to the socket.
		///
		/// This is usually only done when establishing a server
		/// socket. 
		///
		/// If reuseAddress is true, sets the SO_REUSEADDR
		/// socket option.
		///
		/// Calls to connect cannot() come before calls to bind().

	int sendBytes(const void* buffer, int length, int flags = 0);
		/// Sends the contents of the given buffer through
		/// the socket.
		///
		/// Returns the number of bytes sent, which may be
		/// less than the number of bytes specified.

	int receiveBytes(void* buffer, int length, int flags = 0);
		/// Receives data from the socket and stores it
		/// in buffer. Up to length bytes are received.
		///
		/// Returns the number of bytes received.

	int sendTo(const void* buffer, int length, const SocketAddress& address, int flags = 0);
		/// Sends the contents of the given buffer through
		/// the socket to the given address.
		///
		/// Returns the number of bytes sent, which may be
		/// less than the number of bytes specified.

	int receiveFrom(void* buffer, int length, SocketAddress& address, int flags = 0);
		/// Receives data from the socket and stores it
		/// in buffer. Up to length bytes are received.
		/// Stores the address of the sender in address.
		///
		/// Returns the number of bytes received.

	void setBroadcast(bool flag);
		/// Sets the value of the SO_BROADCAST socket option.
		///
		/// Setting this flag allows sending datagrams to
		/// the broadcast address.
	
	bool getBroadcast() const;
		/// Returns the value of the SO_BROADCAST socket option.

protected:
	DatagramSocket(SocketImpl* pImpl);
		/// Creates the Socket and attaches the given SocketImpl.
		/// The socket takes owership of the SocketImpl.
		///
		/// The SocketImpl must be a StreamSocketImpl, otherwise
		/// an InvalidArgumentException will be thrown.
};


//
// inlines
//
inline void DatagramSocket::setBroadcast(bool flag)
{
	impl()->setBroadcast(flag);
}


inline bool DatagramSocket::getBroadcast() const
{
	return impl()->getBroadcast();
}


} } // namespace Poco::Net


#endif // Net_DatagramSocket_INCLUDED
