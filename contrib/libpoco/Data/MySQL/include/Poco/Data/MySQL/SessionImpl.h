//
// SessionImpl.h
//
// $Id: //poco/1.4/Data/MySQL/include/Poco/Data/MySQL/SessionImpl.h#1 $
//
// Library: Data
// Package: MySQL
// Module:  SessionImpl
//
// Definition of the SessionImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_MySQL_SessionImpl_INCLUDED
#define Data_MySQL_SessionImpl_INCLUDED


#include "Poco/Data/MySQL/MySQL.h"
#include "Poco/Data/AbstractSessionImpl.h"
#include "Poco/Data/MySQL/SessionHandle.h"
#include "Poco/Data/MySQL/StatementExecutor.h"
#include "Poco/Data/MySQL/ResultMetadata.h"
#include "Poco/Mutex.h"


namespace Poco {
namespace Data {
namespace MySQL {


class MySQL_API SessionImpl: public Poco::Data::AbstractSessionImpl<SessionImpl>
	/// Implements SessionImpl interface
{
public:
	static const std::string MYSQL_READ_UNCOMMITTED;
	static const std::string MYSQL_READ_COMMITTED;
	static const std::string MYSQL_REPEATABLE_READ;
	static const std::string MYSQL_SERIALIZABLE;

	SessionImpl(const std::string& connectionString,
		std::size_t loginTimeout = LOGIN_TIMEOUT_DEFAULT);
		/// Creates the SessionImpl. Opens a connection to the database
		///
		/// Connection string format:
		///     <str> == <assignment> | <assignment> ';' <str>
		///     <assignment> == <name> '=' <value>
		///     <name> == 'host' | 'port' | 'user' | 'password' | 'db' } 'compress' | 'auto-reconnect'
		///     <value> == [~;]*
		///
		/// for compress and auto-reconnect correct values are true/false
		/// for port - numeric in decimal notation
		///
		
	~SessionImpl();
		/// Destroys the SessionImpl.
		
	Poco::Data::StatementImpl* createStatementImpl();
		/// Returns an MySQL StatementImpl

	void open(const std::string& connection = "");
		/// Opens a connection to the database.

	void close();
		/// Closes the connection.
		
	bool isConnected();
		/// Returns true if connected, false otherwise.

	void setConnectionTimeout(std::size_t timeout);
		/// Sets the session connection timeout value.

	std::size_t getConnectionTimeout();
		/// Returns the session connection timeout value.

	void begin();
		/// Starts a transaction
	
	void commit();
		/// Commits and ends a transaction		

	void rollback();
		/// Aborts a transaction
		
	bool canTransact();
		/// Returns true if session has transaction capabilities.

	bool isTransaction();
		/// Returns true iff a transaction is a transaction is in progress, false otherwise.

	void setTransactionIsolation(Poco::UInt32 ti);
		/// Sets the transaction isolation level.

	Poco::UInt32 getTransactionIsolation();
		/// Returns the transaction isolation level.

	bool hasTransactionIsolation(Poco::UInt32 ti);
		/// Returns true iff the transaction isolation level corresponding
		/// to the supplied bitmask is supported.

	bool isTransactionIsolation(Poco::UInt32 ti);
		/// Returns true iff the transaction isolation level corresponds
		/// to the supplied bitmask.
		
	void autoCommit(const std::string&, bool val);
		/// Sets autocommit property for the session.

	bool isAutoCommit(const std::string& name="");
		/// Returns autocommit property value.

	void setInsertId(const std::string&, const Poco::Any&);
		/// Try to set insert id - do nothing.
		
	Poco::Any getInsertId(const std::string&);
		/// Get insert id

	SessionHandle& handle();
		// Get handle

	const std::string& connectorName() const;
		/// Returns the name of the connector.

private:

	template <typename T>
	inline T& getValue(MYSQL_BIND* pResult, T& val)
	{
		return val = *((T*) pResult->buffer);
	}

	template <typename T>
	T& getSetting(const std::string& name, T& val)
		/// Returns required setting.
		/// Limited to one setting at a time.
	{
		StatementExecutor ex(_handle);
		ResultMetadata metadata;
		metadata.reset();
		ex.prepare(Poco::format("SELECT @@%s", name));
		metadata.init(ex);

		if (metadata.columnsReturned() > 0)
			ex.bindResult(metadata.row());
		else
			throw InvalidArgumentException("No data returned.");

		ex.execute(); ex.fetch();
		MYSQL_BIND* pResult = metadata.row();
		return getValue<T>(pResult, val);
	}

	std::string     _connector;
	SessionHandle   _handle;
	bool            _connected;
	bool            _inTransaction;
	std::size_t     _timeout;
	Poco::FastMutex _mutex;
};


//
// inlines
//
inline bool SessionImpl::canTransact()
{
	return true;
}


inline void SessionImpl::setInsertId(const std::string&, const Poco::Any&)
{
}


inline Poco::Any SessionImpl::getInsertId(const std::string&)
{
	return Poco::Any(Poco::UInt64(mysql_insert_id(_handle)));
}


inline SessionHandle& SessionImpl::handle()
{
	return _handle;
}


inline const std::string& SessionImpl::connectorName() const
{
	return _connector;
}


inline bool SessionImpl::isTransaction()
{
	return _inTransaction;
}


inline bool SessionImpl::isTransactionIsolation(Poco::UInt32 ti)
{
	return getTransactionIsolation() == ti;
}


inline bool SessionImpl::isConnected()
{
	return _connected;
}
	

inline std::size_t SessionImpl::getConnectionTimeout()
{
	return _timeout;
}


template <>
inline std::string& SessionImpl::getValue(MYSQL_BIND* pResult, std::string& val)
{
	val.assign((char*) pResult->buffer, pResult->buffer_length);
	return val;
}


} } } // namespace Poco::Data::MySQL


#endif // Data_MySQL_SessionImpl_INCLUDED
