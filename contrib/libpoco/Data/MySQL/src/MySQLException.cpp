//
// MySQLException.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/MySQLException.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  MySQLException
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/MySQLException.h"
#include <mysql.h>
#include <stdio.h>

namespace Poco {
namespace Data {
namespace MySQL {


MySQLException::MySQLException(const std::string& msg) : Poco::Data::DataException(std::string("[MySQL]: ") + msg)
{
}


MySQLException::MySQLException(const MySQLException& exc) : Poco::Data::DataException(exc)
{
}


MySQLException::~MySQLException() throw()
{
}


/////
//
// ConnectionException
//
/////


ConnectionException::ConnectionException(const std::string& msg) : MySQLException(msg)
{
}


ConnectionException::ConnectionException(const std::string& text, MYSQL* h) : MySQLException(compose(text, h))
{
}


std::string ConnectionException::compose(const std::string& text, MYSQL* h)
{
	std::string str;
	str += "[Comment]: ";
	str += text;
	str += "\t[mysql_error]: ";
	str += mysql_error(h);

	str += "\t[mysql_errno]: ";
	char buff[30];
	sprintf(buff, "%d", mysql_errno(h));
	str += buff;

	str += "\t[mysql_sqlstate]: ";
	str += mysql_sqlstate(h);
	return str;
}


/////
//
// TransactionException
//
/////


TransactionException::TransactionException(const std::string& msg) : ConnectionException(msg)
{
}


TransactionException::TransactionException(const std::string& text, MYSQL* h) : ConnectionException(text, h)
{
}


/////
//
// StatementException
//
/////


StatementException::StatementException(const std::string& msg) : MySQLException(msg)
{
}


StatementException::StatementException(const std::string& text, MYSQL_STMT* h, const std::string& stmt) : MySQLException(compose(text, h, stmt))
{
}


std::string StatementException::compose(const std::string& text, MYSQL_STMT* h, const std::string& stmt)
{
	std::string str;
	str += "[Comment]: ";
	str += text;

	if (h != 0)
	{
		str += "\t[mysql_stmt_error]: ";
		str += mysql_stmt_error(h);

		str += "\t[mysql_stmt_errno]: ";
		char buff[30];
		sprintf(buff, "%d", mysql_stmt_errno(h));
		str += buff;

		str += "\t[mysql_stmt_sqlstate]: ";
		str += mysql_stmt_sqlstate(h);
	}

	if (stmt.length() > 0)
	{
		str += "\t[statemnt]: ";
		str += stmt;
	}

	return str;
}

} } } // namespace Poco::Data::MySQL
