//
// TCPServerConnectionFactory.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/TCPServerConnectionFactory.h#1 $
//
// Library: Net
// Package: TCPServer
// Module:  TCPServerConnectionFactory
//
// Definition of the TCPServerConnectionFactory class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_TCPServerConnectionFactory_INCLUDED
#define Net_TCPServerConnectionFactory_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/TCPServerConnection.h"
#include "Poco/SharedPtr.h"


namespace Poco {
namespace Net {


class Net_API TCPServerConnectionFactory
	/// A factory for TCPServerConnection objects.
	///
	/// The TCPServer class uses a TCPServerConnectionFactory
	/// to create a connection object for each new connection
	/// it accepts.
	///
	/// Subclasses must override the createConnection()
	/// method.
	///
	/// The TCPServerConnectionFactoryImpl template class
	/// can be used to automatically instantiate a
	/// TCPServerConnectionFactory for a given subclass
	/// of TCPServerConnection.
{
public:
	typedef Poco::SharedPtr<TCPServerConnectionFactory> Ptr;
	
	virtual ~TCPServerConnectionFactory();
		/// Destroys the TCPServerConnectionFactory.

	virtual TCPServerConnection* createConnection(const StreamSocket& socket) = 0;
		/// Creates an instance of a subclass of TCPServerConnection,
		/// using the given StreamSocket.

protected:
	TCPServerConnectionFactory();
		/// Creates the TCPServerConnectionFactory.

private:
	TCPServerConnectionFactory(const TCPServerConnectionFactory&);
	TCPServerConnectionFactory& operator = (const TCPServerConnectionFactory&);
};


template <class S>
class TCPServerConnectionFactoryImpl: public TCPServerConnectionFactory
	/// This template provides a basic implementation of
	/// TCPServerConnectionFactory.
{
public:
	TCPServerConnectionFactoryImpl()
	{
	}
	
	~TCPServerConnectionFactoryImpl()
	{
	}
	
	TCPServerConnection* createConnection(const StreamSocket& socket)
	{
		return new S(socket);
	}
};


} } // namespace Poco::Net


#endif // Net_TCPServerConnectionFactory_INCLUDED
