//
// PoolableConnectionFactory.h
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  PoolableConnectionFactory
//
// Definition of the PoolableConnectionFactory class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_PoolableConnectionFactory_INCLUDED
#define MongoDB_PoolableConnectionFactory_INCLUDED


#include "Poco/MongoDB/Connection.h"
#include "Poco/ObjectPool.h"


namespace Poco {


template<>
class PoolableObjectFactory<MongoDB::Connection, MongoDB::Connection::Ptr>
	/// PoolableObjectFactory specialisation for Connection. New connections
	/// are created with the given address.
{
public:
	PoolableObjectFactory(Net::SocketAddress& address)
		: _address(address)
	{
	}

	PoolableObjectFactory(const std::string& address)
		: _address(address)
	{
	}

	MongoDB::Connection::Ptr createObject()
	{
		return new MongoDB::Connection(_address);
	}
	
	bool validateObject(MongoDB::Connection::Ptr pObject)
	{
		return true;
	}

	void activateObject(MongoDB::Connection::Ptr pObject)
	{
	}

	void deactivateObject(MongoDB::Connection::Ptr pObject)
	{
	}

	void destroyObject(MongoDB::Connection::Ptr pObject)
	{
	}

private:
	Net::SocketAddress _address;
};


namespace MongoDB {


class PooledConnection
	/// Helper class for borrowing and returning a connection automatically from a pool.
{
public:
	PooledConnection(Poco::ObjectPool<Connection, Connection::Ptr>& pool) : _pool(pool)
	{
		_connection = _pool.borrowObject();
	}

	virtual ~PooledConnection()
	{
		try
		{
			_pool.returnObject(_connection);
		}
		catch (...)
		{
			poco_unexpected();
		}
	}

	operator Connection::Ptr ()
	{
		return _connection;
	}

private:
	Poco::ObjectPool<Connection, Connection::Ptr>& _pool;
	Connection::Ptr _connection;
};


} // namespace MongoDB
} // namespace Poco


#endif //MongoDB_PoolableConnectionFactory_INCLUDED
