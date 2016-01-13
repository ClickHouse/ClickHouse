//
// StatementExecutor.cpp
//
// $Id: //poco/1.3/Data/MySQL/src/StatementExecutor.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  StatementExecutor
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include <mysql.h>
#include "Poco/Data/MySQL/StatementExecutor.h"
#include "Poco/Format.h"


namespace Poco {
namespace Data {
namespace MySQL {


StatementExecutor::StatementExecutor(MYSQL* mysql)
	: _pSessionHandle(mysql)
	, _affectedRowCount(0)
{
	if (!(_pHandle = mysql_stmt_init(mysql)))
		throw StatementException("mysql_stmt_init error");

	_state = STMT_INITED;
}


StatementExecutor::~StatementExecutor()
{
	mysql_stmt_close(_pHandle);
}


int StatementExecutor::state() const
{
	return _state;
}


void StatementExecutor::prepare(const std::string& query)
{
	if (_state >= STMT_COMPILED)
	{
		_state = STMT_COMPILED;
		return;
	}
	
	if (mysql_stmt_prepare(_pHandle, query.c_str(), static_cast<unsigned int>(query.length())) != 0)
		throw StatementException("mysql_stmt_prepare error", _pHandle, query);

	_query = query;
	_state = STMT_COMPILED;
}


void StatementExecutor::bindParams(MYSQL_BIND* params, std::size_t count)
{
	if (_state < STMT_COMPILED)
		throw StatementException("Statement is not compiled yet");

	if (count != mysql_stmt_param_count(_pHandle))
		throw StatementException("wrong bind parameters count", 0, _query);

	if (count == 0) return;

	if (mysql_stmt_bind_param(_pHandle, params) != 0)
		throw StatementException("mysql_stmt_bind_param() error ", _pHandle, _query);
}


void StatementExecutor::bindResult(MYSQL_BIND* result)
{
	if (_state < STMT_COMPILED)
		throw StatementException("Statement is not compiled yet");

	if (mysql_stmt_bind_result(_pHandle, result) != 0)
		throw StatementException("mysql_stmt_bind_result error ", _pHandle, _query);
}


void StatementExecutor::execute()
{
	if (_state < STMT_COMPILED)
		throw StatementException("Statement is not compiled yet");

	if (mysql_stmt_execute(_pHandle) != 0)
		throw StatementException("mysql_stmt_execute error", _pHandle, _query);

	_state = STMT_EXECUTED;

	my_ulonglong affectedRows = mysql_affected_rows(_pSessionHandle);
	if (affectedRows != ((my_ulonglong) - 1))
		_affectedRowCount = static_cast<std::size_t>(affectedRows); //Was really a DELETE, UPDATE or INSERT statement
}


bool StatementExecutor::fetch()
{
	if (_state < STMT_EXECUTED)
		throw StatementException("Statement is not executed yet");

	int res = mysql_stmt_fetch(_pHandle);

	// we have specified zero buffers for BLOBs, so DATA_TRUNCATED is normal in this case
	if ((res != 0) && (res != MYSQL_NO_DATA) && (res != MYSQL_DATA_TRUNCATED)) 
		throw StatementException("mysql_stmt_fetch error", _pHandle, _query);

	return (res == 0) || (res == MYSQL_DATA_TRUNCATED);
}


bool StatementExecutor::fetchColumn(std::size_t n, MYSQL_BIND *bind)
{
	if (_state < STMT_EXECUTED)
		throw StatementException("Statement is not executed yet");

	int res = mysql_stmt_fetch_column(_pHandle, bind, static_cast<unsigned int>(n), 0);

	if ((res != 0) && (res != MYSQL_NO_DATA))
		throw StatementException(Poco::format("mysql_stmt_fetch_column(%z) error", n), _pHandle, _query);

	return (res == 0);
}

int StatementExecutor::getAffectedRowCount() const
{
	return _affectedRowCount;
}


}}}
