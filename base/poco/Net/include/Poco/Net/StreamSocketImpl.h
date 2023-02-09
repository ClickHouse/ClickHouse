//
// StreamSocketImpl.h
//
// Library: Net
// Package: Sockets
// Module:  StreamSocketImpl
//
// Definition of the StreamSocketImpl class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_StreamSocketImpl_INCLUDED
#define Net_StreamSocketImpl_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketImpl.h"


namespace Poco {
namespace Net {


class Net_API StreamSocketImpl: public SocketImpl
	/// This class implements a TCP socket.
{
public:
	StreamSocketImpl();
		/// Creates a StreamSocketImpl.

	explicit StreamSocketImpl(SocketAddress::Family addressFamily);
		/// Creates a SocketImpl, with the underlying
		/// socket initialized for the given address family.
		
	StreamSocketImpl(poco_socket_t sockfd);
		/// Creates a StreamSocketImpl using the given native socket.
		
	virtual int sendBytes(const void* buffer, int length, int flags = 0);
		/// Ensures that all data in buffer is sent if the socket
		/// is blocking. In case of a non-blocking socket, sends as
		/// many bytes as possible.
		///
		/// Returns the number of bytes sent. The return value may also be
		/// negative to denote some special condition.

protected:
	virtual ~StreamSocketImpl();
};


} } // namespace Poco::Net


#endif // Net_StreamSocketImpl_INCLUDED
