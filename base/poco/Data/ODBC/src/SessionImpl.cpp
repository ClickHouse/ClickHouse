//
// SessionImpl.cpp
//
// Library: Data/ODBC
// Package: ODBC
// Module:  SessionImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/SessionImpl.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/ODBCStatementImpl.h"
#include "Poco/Data/ODBC/Error.h"
#include "Poco/Data/ODBC/ODBCException.h"
#include "Poco/Data/Session.h"
#include "Poco/String.h"
#include <sqlext.h>


namespace Poco {
namespace Data {
namespace ODBC {


SessionImpl::SessionImpl(const std::string& connect,
	std::size_t loginTimeout,
	std::size_t maxFieldSize,
	bool autoBind,
	bool autoExtract): 
	Poco::Data::AbstractSessionImpl<SessionImpl>(connect, loginTimeout),
		_connector(Connector::KEY),
		_maxFieldSize(maxFieldSize),
		_autoBind(autoBind),
		_autoExtract(autoExtract),
		_canTransact(ODBC_TXN_CAPABILITY_UNKNOWN),
		_inTransaction(false),
		_queryTimeout(-1)
{
	setFeature("bulk", true);
	open();
	setProperty("handle", _db.handle());
}


SessionImpl::SessionImpl(const std::string& connect,
	Poco::Any maxFieldSize, 
	bool enforceCapability,
	bool autoBind,
	bool autoExtract): Poco::Data::AbstractSessionImpl<SessionImpl>(connect),
		_connector(Connector::KEY),
		_maxFieldSize(maxFieldSize),
		_autoBind(autoBind),
		_autoExtract(autoExtract),
		_canTransact(ODBC_TXN_CAPABILITY_UNKNOWN),
		_inTransaction(false),
		_queryTimeout(-1)
{
	setFeature("bulk", true);
	open();
	setProperty("handle", _db.handle());
}


SessionImpl::~SessionImpl()
{
	try
	{
		if (isTransaction() && !getFeature("autoCommit"))
		{
			try { rollback(); }
			catch (...) { }
		}

		close();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


Poco::Data::StatementImpl* SessionImpl::createStatementImpl()
{
	return new ODBCStatementImpl(*this);
}


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

	SQLULEN tout = static_cast<SQLULEN>(getLoginTimeout());
	if (Utility::isError(SQLSetConnectAttr(_db, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) tout, 0)))
	{
		if (Utility::isError(SQLGetConnectAttr(_db, SQL_ATTR_LOGIN_TIMEOUT, &tout, 0, 0)) ||
				getLoginTimeout() != tout)
		{
			ConnectionError e(_db);
			throw ConnectionFailedException(e.toString());
		}
	}

	SQLCHAR connectOutput[512] = {0};
	SQLSMALLINT result;

	if (Utility::isError(Poco::Data::ODBC::SQLDriverConnect(_db
		, NULL
		,(SQLCHAR*) connectionString().c_str()
		,(SQLSMALLINT) SQL_NTS
		, connectOutput
		, sizeof(connectOutput)
		, &result
		, SQL_DRIVER_NOPROMPT)))
	{
		ConnectionError err(_db);
		std::string errStr = err.toString();
		close();
		throw ConnectionFailedException(errStr);
	}

	_dataTypes.fillTypeInfo(_db);
		addProperty("dataTypeInfo", 
		&SessionImpl::setDataTypeInfo, 
		&SessionImpl::dataTypeInfo);

	addFeature("autoCommit", 
		&SessionImpl::autoCommit, 
		&SessionImpl::isAutoCommit);

	addFeature("autoBind", 
		&SessionImpl::autoBind, 
		&SessionImpl::isAutoBind);

	addFeature("autoExtract", 
		&SessionImpl::autoExtract, 
		&SessionImpl::isAutoExtract);

	addProperty("maxFieldSize",
		&SessionImpl::setMaxFieldSize,
		&SessionImpl::getMaxFieldSize);

	addProperty("queryTimeout",
		&SessionImpl::setQueryTimeout,
		&SessionImpl::getQueryTimeout);

	Poco::Data::ODBC::SQLSetConnectAttr(_db, SQL_ATTR_QUIET_MODE, 0, 0);

	if (!canTransact()) autoCommit("", true);
}


bool SessionImpl::isConnected()
{
	SQLULEN value = 0;

	if (Utility::isError(Poco::Data::ODBC::SQLGetConnectAttr(_db,
		SQL_ATTR_CONNECTION_DEAD,
		&value,
		0,
		0))) return false;

	return (SQL_CD_FALSE == value);
}


void SessionImpl::setConnectionTimeout(std::size_t timeout)
{
	SQLUINTEGER value = static_cast<SQLUINTEGER>(timeout);

	checkError(Poco::Data::ODBC::SQLSetConnectAttr(_db,
		SQL_ATTR_CONNECTION_TIMEOUT,
		&value,
		SQL_IS_UINTEGER), "Failed to set connection timeout.");
}


std::size_t SessionImpl::getConnectionTimeout()
{
	SQLULEN value = 0;

	checkError(Poco::Data::ODBC::SQLGetConnectAttr(_db,
		SQL_ATTR_CONNECTION_TIMEOUT,
		&value,
		0,
		0), "Failed to get connection timeout.");

	return value;
}


bool SessionImpl::canTransact()
{
	if (ODBC_TXN_CAPABILITY_UNKNOWN == _canTransact)
	{
		SQLUSMALLINT ret;
		checkError(Poco::Data::ODBC::SQLGetInfo(_db, SQL_TXN_CAPABLE, &ret, 0, 0), 
			"Failed to obtain transaction capability info.");

		_canTransact = (SQL_TC_NONE != ret) ? 
			ODBC_TXN_CAPABILITY_TRUE : 
			ODBC_TXN_CAPABILITY_FALSE;
	}

	return ODBC_TXN_CAPABILITY_TRUE == _canTransact;
}


void SessionImpl::setTransactionIsolation(Poco::UInt32 ti)
{
#if POCO_PTR_IS_64_BIT
	Poco::UInt64 isolation = 0;
#else
	Poco::UInt32 isolation = 0;
#endif

	if (ti & Session::TRANSACTION_READ_UNCOMMITTED)
		isolation |= SQL_TXN_READ_UNCOMMITTED;

	if (ti & Session::TRANSACTION_READ_COMMITTED)
		isolation |= SQL_TXN_READ_COMMITTED;

	if (ti & Session::TRANSACTION_REPEATABLE_READ)
		isolation |= SQL_TXN_REPEATABLE_READ;

	if (ti & Session::TRANSACTION_SERIALIZABLE)
		isolation |= SQL_TXN_SERIALIZABLE;

	checkError(SQLSetConnectAttr(_db, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER) isolation, 0));
}


Poco::UInt32 SessionImpl::getTransactionIsolation()
{
	SQLULEN isolation = 0;
	checkError(SQLGetConnectAttr(_db, SQL_ATTR_TXN_ISOLATION,
		&isolation,
		0,
		0));

	return transactionIsolation(isolation);
}


bool SessionImpl::hasTransactionIsolation(Poco::UInt32 ti)
{
	if (isTransaction()) throw InvalidAccessException();

	bool retval = true;
	Poco::UInt32 old = getTransactionIsolation();
	try { setTransactionIsolation(ti); }
	catch (Poco::Exception&) { retval = false; }
	setTransactionIsolation(old);
	return retval;
}


Poco::UInt32 SessionImpl::getDefaultTransactionIsolation()
{
	SQLUINTEGER isolation = 0;
	checkError(SQLGetInfo(_db, SQL_DEFAULT_TXN_ISOLATION,
		&isolation,
		0,
		0));

	return transactionIsolation(isolation);
}


Poco::UInt32 SessionImpl::transactionIsolation(SQLULEN isolation)
{
	if (0 == isolation)
		throw InvalidArgumentException("transactionIsolation(SQLUINTEGER)");

	Poco::UInt32 ret = 0;

	if (isolation & SQL_TXN_READ_UNCOMMITTED)
		ret |= Session::TRANSACTION_READ_UNCOMMITTED;

	if (isolation & SQL_TXN_READ_COMMITTED)
		ret |= Session::TRANSACTION_READ_COMMITTED;

	if (isolation & SQL_TXN_REPEATABLE_READ)
		ret |= Session::TRANSACTION_REPEATABLE_READ;

	if (isolation & SQL_TXN_SERIALIZABLE)
		ret |= Session::TRANSACTION_SERIALIZABLE;

	if (0 == ret)
		throw InvalidArgumentException("transactionIsolation(SQLUINTEGER)");

	return ret;
}


void SessionImpl::autoCommit(const std::string&, bool val)
{
	checkError(Poco::Data::ODBC::SQLSetConnectAttr(_db, 
		SQL_ATTR_AUTOCOMMIT, 
		val ? (SQLPOINTER) SQL_AUTOCOMMIT_ON : 
			(SQLPOINTER) SQL_AUTOCOMMIT_OFF, 
		SQL_IS_UINTEGER), "Failed to set automatic commit.");
}


bool SessionImpl::isAutoCommit(const std::string&)
{
	SQLULEN value = 0;

	checkError(Poco::Data::ODBC::SQLGetConnectAttr(_db,
		SQL_ATTR_AUTOCOMMIT,
		&value,
		0,
		0));

	return (0 != value);
}


bool SessionImpl::isTransaction()
{
	if (!canTransact()) return false;

	SQLULEN value = 0;
	checkError(Poco::Data::ODBC::SQLGetConnectAttr(_db,
		SQL_ATTR_AUTOCOMMIT,
		&value,
		0,
		0));

	if (0 == value) return _inTransaction;
	else return false;
}


void SessionImpl::begin()
{
	if (isAutoCommit())
		throw InvalidAccessException("Session in auto commit mode.");

	{
		Poco::FastMutex::ScopedLock l(_mutex);

		if (_inTransaction)
			throw InvalidAccessException("Transaction in progress.");

		_inTransaction = true;
	}
}


void SessionImpl::commit()
{
	if (!isAutoCommit())
		checkError(SQLEndTran(SQL_HANDLE_DBC, _db, SQL_COMMIT));

	_inTransaction = false;
}


void SessionImpl::rollback()
{
	if (!isAutoCommit())
		checkError(SQLEndTran(SQL_HANDLE_DBC, _db, SQL_ROLLBACK));

	_inTransaction = false;
}


void SessionImpl::close()
{
	if (!isConnected()) return;

	try
	{
		commit();
	}
	catch (ConnectionException&) 
	{
	}

	SQLDisconnect(_db);
}


int SessionImpl::maxStatementLength()
{
	SQLUINTEGER info;
	SQLRETURN rc = 0;
	if (Utility::isError(rc = Poco::Data::ODBC::SQLGetInfo(_db,
		SQL_MAXIMUM_STATEMENT_LENGTH,
		(SQLPOINTER) &info,
		0,
		0)))
	{
		throw ConnectionException(_db, 
			"SQLGetInfo(SQL_MAXIMUM_STATEMENT_LENGTH)");
	}

	return info;
}


} } } // namespace Poco::Data::ODBC
