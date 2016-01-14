//
// SessionImpl.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/SessionImpl.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  SessionImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/SessionImpl.h"
#include "Poco/Data/MySQL/MySQLStatementImpl.h"
#include "Poco/Data/Session.h"
#include "Poco/NumberParser.h"
#include "Poco/String.h"


namespace
{
	std::string copyStripped(std::string::const_iterator from, std::string::const_iterator to)
	{
		// skip leading spaces
		while ((from != to) && isspace(*from)) from++;
		// skip trailing spaces
		while ((from != to) && isspace(*(to - 1))) to--;

		return std::string(from, to);
	}
}


namespace Poco {
namespace Data {
namespace MySQL {


const std::string SessionImpl::MYSQL_READ_UNCOMMITTED = "READ UNCOMMITTED";
const std::string SessionImpl::MYSQL_READ_COMMITTED = "READ COMMITTED";
const std::string SessionImpl::MYSQL_REPEATABLE_READ = "REPEATABLE READ";
const std::string SessionImpl::MYSQL_SERIALIZABLE = "SERIALIZABLE";


SessionImpl::SessionImpl(const std::string& connectionString, std::size_t loginTimeout) : 
	Poco::Data::AbstractSessionImpl<SessionImpl>(connectionString, loginTimeout),
	_handle(0),
	_connected(false),
	_inTransaction(false)
{
	addProperty("insertId", &SessionImpl::setInsertId, &SessionImpl::getInsertId);
	setProperty("handle", static_cast<MYSQL*>(_handle));
	open();
	setConnectionTimeout(CONNECTION_TIMEOUT_DEFAULT);
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

	_handle.init();
	
	unsigned int timeout = static_cast<unsigned int>(getLoginTimeout());
	_handle.options(MYSQL_OPT_CONNECT_TIMEOUT, timeout);

	std::map<std::string, std::string> options;

	// Default values
	options["host"] = "localhost";
	options["port"] = "3306";
	options["user"] = "";
	options["password"] = "";
	options["db"] = "";
	options["compress"] = "";
	options["auto-reconnect"] = "";
	options["secure-auth"] = "";
	options["character-set"] = "utf8";

	const std::string& connString = connectionString();
	for (std::string::const_iterator start = connString.begin();;) 
	{
		std::string::const_iterator finish = std::find(start, connString.end(), ';');
		std::string::const_iterator middle = std::find(start, finish, '=');

		if (middle == finish)
			throw MySQLException("create session: bad connection string format, can not find '='");

		options[copyStripped(start, middle)] = copyStripped(middle + 1, finish);

		if ((finish == connString.end()) || (finish + 1 == connString.end())) break;

		start = finish + 1;
	} 

	if (options["user"].empty())
		throw MySQLException("create session: specify user name");

	const char * db = NULL;
	if (!options["db"].empty())
		db = options["db"].c_str();

	unsigned int port = 0;
	if (!NumberParser::tryParseUnsigned(options["port"], port) || 0 == port || port > 65535)
		throw MySQLException("create session: specify correct port (numeric in decimal notation)");

	if (options["compress"] == "true")
		_handle.options(MYSQL_OPT_COMPRESS);
	else if (options["compress"] == "false")
		;
	else if (!options["compress"].empty())
		throw MySQLException("create session: specify correct compress option (true or false) or skip it");

	if (options["auto-reconnect"] == "true")
		_handle.options(MYSQL_OPT_RECONNECT, true);
	else if (options["auto-reconnect"] == "false")
		_handle.options(MYSQL_OPT_RECONNECT, false);
	else if (!options["auto-reconnect"].empty())
		throw MySQLException("create session: specify correct auto-reconnect option (true or false) or skip it");

	if (options["secure-auth"] == "true")
		_handle.options(MYSQL_SECURE_AUTH, true);
	else if (options["secure-auth"] == "false")
		_handle.options(MYSQL_SECURE_AUTH, false);
	else if (!options["secure-auth"].empty())
		throw MySQLException("create session: specify correct secure-auth option (true or false) or skip it");

	if (!options["character-set"].empty())
		_handle.options(MYSQL_SET_CHARSET_NAME, options["character-set"].c_str());

	// Real connect
	_handle.connect(options["host"].c_str(), 
			options["user"].c_str(), 
			options["password"].c_str(), 
			db, 
			port);

	addFeature("autoCommit", 
		&SessionImpl::autoCommit, 
		&SessionImpl::isAutoCommit);

	_connected = true;
}
	

SessionImpl::~SessionImpl()
{
	close();
}
	

Poco::Data::StatementImpl* SessionImpl::createStatementImpl()
{
	return new MySQLStatementImpl(*this);
}	


void SessionImpl::begin()
{
	Poco::FastMutex::ScopedLock l(_mutex);

	if (_inTransaction)
		throw Poco::InvalidAccessException("Already in transaction.");

	_handle.startTransaction();
	_inTransaction = true;
}


void SessionImpl::commit()
{
	_handle.commit();
	_inTransaction = false;
}
	

void SessionImpl::rollback()
{
	_handle.rollback();
	_inTransaction = false;
}


void SessionImpl::autoCommit(const std::string&, bool val)
{
	StatementExecutor ex(_handle);
	ex.prepare(Poco::format("SET autocommit=%d", val ? 1 : 0));
	ex.execute();
}


bool SessionImpl::isAutoCommit(const std::string&)
{
	int ac = 0;
	return 1 == getSetting("autocommit", ac);
}


void SessionImpl::setTransactionIsolation(Poco::UInt32 ti)
{
	std::string isolation;
	switch (ti)
	{
	case Session::TRANSACTION_READ_UNCOMMITTED:
		isolation = MYSQL_READ_UNCOMMITTED; break;
	case Session::TRANSACTION_READ_COMMITTED:
		isolation = MYSQL_READ_COMMITTED; break;
	case Session::TRANSACTION_REPEATABLE_READ:
		isolation = MYSQL_REPEATABLE_READ; break;
	case Session::TRANSACTION_SERIALIZABLE:
		isolation = MYSQL_SERIALIZABLE; break;
	default:
		throw Poco::InvalidArgumentException("setTransactionIsolation()");
	}

	StatementExecutor ex(_handle);
	ex.prepare(Poco::format("SET SESSION TRANSACTION ISOLATION LEVEL %s", isolation));
	ex.execute();
}


Poco::UInt32 SessionImpl::getTransactionIsolation()
{
	std::string isolation;
	getSetting("tx_isolation", isolation);
	Poco::replaceInPlace(isolation, "-", " ");
	if (MYSQL_READ_UNCOMMITTED == isolation)
		return Session::TRANSACTION_READ_UNCOMMITTED;
	else if (MYSQL_READ_COMMITTED == isolation)
		return Session::TRANSACTION_READ_COMMITTED;
	else if (MYSQL_REPEATABLE_READ == isolation)
		return Session::TRANSACTION_REPEATABLE_READ;
	else if (MYSQL_SERIALIZABLE == isolation)
		return Session::TRANSACTION_SERIALIZABLE;

	throw InvalidArgumentException("getTransactionIsolation()");
}


bool SessionImpl::hasTransactionIsolation(Poco::UInt32 ti)
{
	return Session::TRANSACTION_READ_UNCOMMITTED == ti ||
		Session::TRANSACTION_READ_COMMITTED == ti ||
		Session::TRANSACTION_REPEATABLE_READ == ti ||
		Session::TRANSACTION_SERIALIZABLE == ti;
}
	

void SessionImpl::close()
{
	if (_connected)
	{
		_handle.close();
		_connected = false;
	}
}


void SessionImpl::setConnectionTimeout(std::size_t timeout)
{
	_handle.options(MYSQL_OPT_READ_TIMEOUT, static_cast<unsigned int>(timeout));
	_handle.options(MYSQL_OPT_WRITE_TIMEOUT, static_cast<unsigned int>(timeout));
	_timeout = timeout;
}


}}}
