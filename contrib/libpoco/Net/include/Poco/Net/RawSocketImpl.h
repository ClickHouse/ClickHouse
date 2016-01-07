//
// RawSocketImpl.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/RawSocketImpl.h#1 $
//
// Library: Net
// Package: Sockets
// Module:  RawSocketImpl
//
// Definition of the RawSocketImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_RawSocketImpl_INCLUDED
#define Net_RawSocketImpl_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketImpl.h"


namespace Poco {
namespace Net {


class Net_API RawSocketImpl: public SocketImpl
	/// This class implements a raw socket.
{
public:
	RawSocketImpl();
		/// Creates an unconnected IPv4 raw socket with IPPROTO_RAW.
		
	RawSocketImpl(IPAddress::Family family, int proto = IPPROTO_RAW);
		/// Creates an unconnected raw socket.
		///
		/// The socket will be created for the
		/// given address family.

	RawSocketImpl(poco_socket_t sockfd);
		/// Creates a RawSocketImpl using the given native socket.
				
protected:
	void init(int af);
	void init2(int af, int proto);
	
	~RawSocketImpl();
};


} } // namespace Poco::Net


#endif // Net_RawSocketImpl_INCLUDED
