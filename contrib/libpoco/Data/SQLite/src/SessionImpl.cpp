//
// SessionImpl.cpp
//
// $Id: //poco/Main/Data/SQLite/src/SessionImpl.cpp#5 $
//
// Library: SQLite
// Package: SQLite
// Module:  SessionImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLite/SessionImpl.h"
#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/SQLite/SQLiteStatementImpl.h"
#include "Poco/Data/SQLite/SQLiteException.h"
#include "Poco/Data/Session.h"
#include "Poco/ActiveMethod.h"
#include "Poco/ActiveResult.h"
#include "Poco/String.h"
#include "Poco/Mutex.h"
#include "Poco/Data/DataException.h"
#if defined(POCO_UNBUNDLED)
#include <sqlite3.h>
#else
#include "sqlite3.h"
#endif
#include <cstdlib>


#ifndef SQLITE_OPEN_URI
#define SQLITE_OPEN_URI 0
#endif


namespace Poco {
namespace Data {
namespace SQLite {


const std::string SessionImpl::DEFERRED_BEGIN_TRANSACTION("BEGIN DEFERRED");
const std::string SessionImpl::COMMIT_TRANSACTION("COMMIT");
const std::string SessionImpl::ABORT_TRANSACTION("ROLLBACK");


SessionImpl::SessionImpl(const std::string& fileName, std::size_t loginTimeout):
	Poco::Data::AbstractSessionImpl<SessionImpl>(fileName, loginTimeout),
	_connector(Connector::KEY),
	_pDB(0),
	_connected(false),
	_isTransaction(false)
{
	open();
	setConnectionTimeout(CONNECTION_TIMEOUT_DEFAULT);
	setProperty("handle", _pDB);
	addFeature("autoCommit", 
		&SessionImpl::autoCommit, 
		&SessionImpl::isAutoCommit);
	addProperty("connectionTimeout", &SessionImpl::setConnectionTimeout, &SessionImpl::getConnectionTimeout);
}


SessionImpl::~SessionImpl()
{
	try
	{
		close();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


Poco::Data::StatementImpl* SessionImpl::createStatementImpl()
{
	poco_check_ptr (_pDB);
	return new SQLiteStatementImpl(*this, _pDB);
}


void SessionImpl::begin()
{
	Poco::Mutex::ScopedLock l(_mutex);
	SQLiteStatementImpl tmp(*this, _pDB);
	tmp.add(DEFERRED_BEGIN_TRANSACTION);
	tmp.execute();
	_isTransaction = true;
}


void SessionImpl::commit()
{
	Poco::Mutex::ScopedLock l(_mutex);
	SQLiteStatementImpl tmp(*this, _pDB);
	tmp.add(COMMIT_TRANSACTION);
	tmp.execute();
	_isTransaction = false;
}


void SessionImpl::rollback()
{
	Poco::Mutex::ScopedLock l(_mutex);
	SQLiteStatementImpl tmp(*this, _pDB);
	tmp.add(ABORT_TRANSACTION);
	tmp.execute();
	_isTransaction = false;
}


void SessionImpl::setTransactionIsolation(Poco::UInt32 ti)
{
	if (ti != Session::TRANSACTION_READ_COMMITTED)
		throw Poco::InvalidArgumentException("setTransactionIsolation()");
}


Poco::UInt32 SessionImpl::getTransactionIsolation()
{
	return Session::TRANSACTION_READ_COMMITTED;
}


bool SessionImpl::hasTransactionIsolation(Poco::UInt32 ti)
{
	if (ti == Session::TRANSACTION_READ_COMMITTED) return true;
	return false;
}


bool SessionImpl::isTransactionIsolation(Poco::UInt32 ti)
{
	if (ti == Session::TRANSACTION_READ_COMMITTED) return true;
	return false;
}


class ActiveConnector
{
public:
	ActiveConnector(const std::string& connectString, sqlite3** ppDB):
		connect(this, &ActiveConnector::connectImpl),
		_connectString(connectString),
		_ppDB(ppDB)
	{
		poco_check_ptr(_ppDB);
	}
	
	ActiveMethod<int, void, ActiveConnector> connect;
	
private:
	ActiveConnector();

	inline int connectImpl()
	{
		return sqlite3_open_v2(_connectString.c_str(), _ppDB, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
	}

	std::string _connectString;
	sqlite3**   _ppDB;
};


void SessionImpl::open(const std::string& connect)
{
	if (connect != connectionString())
	{
		if (isConnected())
			throw InvalidAccessException("Session already connected");

		if (!connect.empty())
			setConnectionString(connect);
	}

	poco_assert_dbg (!connectionString().empty());

	try
	{
		ActiveConnector connector(connectionString(), &_pDB);
		ActiveResult<int> result = connector.connect();
		if (!result.tryWait(getLoginTimeout() * 1000))
			throw ConnectionFailedException("Timed out.");

		int rc = result.data();
		if (rc != 0)
		{
			close();
			Utility::throwException(rc);
		}
	} 
	catch (SQLiteException& ex)
	{
		throw ConnectionFailedException(ex.displayText());
	}

	_connected = true;
}


void SessionImpl::close()
{
	if (_pDB)
	{
		sqlite3_close(_pDB);
		_pDB = 0;
	}

	_connected = false;
}


bool SessionImpl::isConnected()
{
	return _connected;
}


void SessionImpl::setConnectionTimeout(std::size_t timeout)
{
	int tout = 1000 * timeout;
	int rc = sqlite3_busy_timeout(_pDB, tout);
	if (rc != 0) Utility::throwException(rc);
	_timeout = tout;
}


void SessionImpl::setConnectionTimeout(const std::string& prop, const Poco::Any& value)
{
	setConnectionTimeout(Poco::RefAnyCast<std::size_t>(value));
}


Poco::Any SessionImpl::getConnectionTimeout(const std::string& prop)
{
	return Poco::Any(_timeout/1000);
}


void SessionImpl::autoCommit(const std::string&, bool)
{
	// The problem here is to decide whether to call commit or rollback
	// when autocommit is set to true. Hence, it is best not to implement
	// this explicit call and only implicitly support autocommit setting.
	throw NotImplementedException(
		"SQLite autocommit is implicit with begin/commit/rollback.");
}


bool SessionImpl::isAutoCommit(const std::string&)
{
	Poco::Mutex::ScopedLock l(_mutex);
	return (0 != sqlite3_get_autocommit(_pDB));
}


// NOTE: Utility::dbHandle() has been moved here from Utility.cpp
// as a workaround for a failing AnyCast with Clang.
// See <https://github.com/pocoproject/poco/issues/578>
// for a discussion.
sqlite3* Utility::dbHandle(const Session& session)
{
	return AnyCast<sqlite3*>(session.getProperty("handle"));
}


} } } // namespace Poco::Data::SQLite
