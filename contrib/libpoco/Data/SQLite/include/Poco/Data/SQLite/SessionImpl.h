//
// SessionImpl.h
//
// $Id: //poco/Main/Data/SQLite/include/Poco/Data/SQLite/SessionImpl.h#2 $
//
// Library: SQLite
// Package: SQLite
// Module:  SessionImpl
//
// Definition of the SessionImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_SQLite_SessionImpl_INCLUDED
#define Data_SQLite_SessionImpl_INCLUDED


#include "Poco/Data/SQLite/SQLite.h"
#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/SQLite/Binder.h"
#include "Poco/Data/AbstractSessionImpl.h"
#include "Poco/SharedPtr.h"
#include "Poco/Mutex.h"


extern "C"
{
	typedef struct sqlite3 sqlite3;
}


namespace Poco {
namespace Data {
namespace SQLite {


class SQLite_API SessionImpl: public Poco::Data::AbstractSessionImpl<SessionImpl>
	/// Implements SessionImpl interface.
{
public:
	SessionImpl(const std::string& fileName,
		std::size_t loginTimeout = LOGIN_TIMEOUT_DEFAULT);
		/// Creates the SessionImpl. Opens a connection to the database.

	~SessionImpl();
		/// Destroys the SessionImpl.

	Poco::Data::StatementImpl* createStatementImpl();
		/// Returns an SQLite StatementImpl.

	void open(const std::string& connect = "");
		/// Opens a connection to the Database.
		/// 
		/// An in-memory system database (sys), with a single table (dual) 
		/// containing single field (dummy) is attached to the database.
		/// The in-memory system database is used to force change count
		/// to be reset to zero on every new query (or batch of queries) 
		/// execution. Without this functionality, select statements
		/// executions that do not return any rows return the count of
		/// changes effected by the most recent insert, update or delete.
		/// In-memory system database can be queried and updated but can not
		/// be dropped. It may be used for other purposes 
		/// in the future.

	void close();
		/// Closes the session.

	bool isConnected();
		/// Returns true if connected, false otherwise.

	void setConnectionTimeout(std::size_t timeout);
		/// Sets the session connection timeout value.

	std::size_t getConnectionTimeout();
		/// Returns the session connection timeout value.

	void begin();
		/// Starts a transaction.

	void commit();
		/// Commits and ends a transaction.

	void rollback();
		/// Aborts a transaction.

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

	const std::string& connectorName() const;
		/// Returns the name of the connector.

protected:
	void setConnectionTimeout(const std::string& prop, const Poco::Any& value);
	Poco::Any getConnectionTimeout(const std::string& prop);

private:
	std::string _connector;
	sqlite3*    _pDB;
	bool        _connected;
	bool        _isTransaction;
	int         _timeout;
	Poco::Mutex _mutex;

	static const std::string DEFERRED_BEGIN_TRANSACTION;
	static const std::string COMMIT_TRANSACTION;
	static const std::string ABORT_TRANSACTION;
};


//
// inlines
//
inline bool SessionImpl::canTransact()
{
	return true;
}


inline 	bool SessionImpl::isTransaction()
{
	return _isTransaction;
}


inline const std::string& SessionImpl::connectorName() const
{
	return _connector;
}


inline std::size_t SessionImpl::getConnectionTimeout()
{
	return static_cast<std::size_t>(_timeout);
}


} } } // namespace Poco::Data::SQLite


#endif // Data_SQLite_SessionImpl_INCLUDED
