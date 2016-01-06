//
// SQLiteStatementImpl.cpp
//
// $Id: //poco/Main/Data/SQLite/src/SQLiteStatementImpl.cpp#8 $
//
// Library: SQLite
// Package: SQLite
// Module:  SQLiteStatementImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLite/SQLiteStatementImpl.h"
#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/SQLite/SQLiteException.h"
#include "Poco/String.h"
#include <cstdlib>
#include <cstring>
#if defined(POCO_UNBUNDLED)
#include <sqlite3.h>
#else
#include "sqlite3.h"
#endif


namespace Poco {
namespace Data {
namespace SQLite {


const int SQLiteStatementImpl::POCO_SQLITE_INV_ROW_CNT = -1;


SQLiteStatementImpl::SQLiteStatementImpl(Poco::Data::SessionImpl& rSession, sqlite3* pDB):
	StatementImpl(rSession),
	_pDB(pDB),
	_pStmt(0),
	_stepCalled(false),
	_nextResponse(0),
	_affectedRowCount(POCO_SQLITE_INV_ROW_CNT),
	_canBind(false),
	_isExtracted(false),
	_canCompile(true)
{
	_columns.resize(1);
}
	

SQLiteStatementImpl::~SQLiteStatementImpl()
{
	try
	{
		clear();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void SQLiteStatementImpl::compileImpl()
{
	if (!_pLeftover)
	{
		_bindBegin = bindings().begin();
	}

	std::string statement(toString());

	sqlite3_stmt* pStmt = 0;
	const char* pSql = _pLeftover ? _pLeftover->c_str() : statement.c_str();

	if (0 == std::strlen(pSql))
		throw InvalidSQLStatementException("Empty statements are illegal");

	int rc = SQLITE_OK;
	const char* pLeftover = 0;
	bool queryFound = false;

	do
	{
		rc = sqlite3_prepare_v2(_pDB, pSql, -1, &pStmt, &pLeftover);
		if (rc != SQLITE_OK)
		{
			if (pStmt) sqlite3_finalize(pStmt);
			pStmt = 0;
			std::string errMsg = sqlite3_errmsg(_pDB);
			Utility::throwException(rc, errMsg);
		}
		else if (rc == SQLITE_OK && pStmt)
		{
			queryFound = true;
		}
		else if (rc == SQLITE_OK && !pStmt) // comment/whitespace ignore
		{
			pSql = pLeftover;
			if (std::strlen(pSql) == 0)
			{
				// empty statement or an conditional statement! like CREATE IF NOT EXISTS
				// this is valid
				queryFound = true;
			}
		}
	} while (rc == SQLITE_OK && !pStmt && !queryFound);

	//Finalization call in clear() invalidates the pointer, so the value is remembered here.
	//For last statement in a batch (or a single statement), pLeftover == "", so the next call
	// to compileImpl() shall return false immediately when there are no more statements left.
	std::string leftOver(pLeftover);
	trimInPlace(leftOver);
	clear();
	_pStmt = pStmt;
	if (!leftOver.empty())
	{
		_pLeftover = new std::string(leftOver);
		_canCompile = true;
	}
	else _canCompile = false;

	_pBinder = new Binder(_pStmt);
	_pExtractor = new Extractor(_pStmt);

	if (SQLITE_DONE == _nextResponse && _isExtracted)
	{
		//if this is not the first compile and there has already been extraction
		//during previous step, switch to the next set if there is one provided
		if (hasMoreDataSets())
		{
			activateNextDataSet();
			_isExtracted = false;
		}
	}

	int colCount = sqlite3_column_count(_pStmt);

	if (colCount)
	{
		std::size_t curDataSet = currentDataSet();
		if (curDataSet >= _columns.size()) _columns.resize(curDataSet + 1);
		for (int i = 0; i < colCount; ++i)
		{
			MetaColumn mc(i, sqlite3_column_name(_pStmt, i), Utility::getColumnType(_pStmt, i));
				_columns[curDataSet].push_back(mc);
		}
	}
}


void SQLiteStatementImpl::bindImpl()
{
	_stepCalled = false;
	_nextResponse = 0;
	if (_pStmt == 0) return;

	sqlite3_reset(_pStmt);

	int paramCount = sqlite3_bind_parameter_count(_pStmt);
	BindIt bindEnd = bindings().end();
	if (0 == paramCount || bindEnd == _bindBegin) 
	{
		_canBind = false;
		return;
	}

	std::size_t availableCount = 0;
	Bindings::difference_type bindCount = 0;
	Bindings::iterator it = _bindBegin;
	for (; it != bindEnd; ++it)
	{
		availableCount += (*it)->numOfColumnsHandled();
		if (availableCount <= paramCount) ++bindCount;
		else break;
	}

	Bindings::difference_type remainingBindCount = bindEnd - _bindBegin;
	if (bindCount < remainingBindCount)
	{
		bindEnd = _bindBegin + bindCount;
		_canBind = true;
	}
	else if (bindCount > remainingBindCount)
		throw ParameterCountMismatchException();

	std::size_t boundRowCount;
	if (_bindBegin != bindings().end())
	{
		boundRowCount = (*_bindBegin)->numOfRowsHandled();

		Bindings::iterator oldBegin = _bindBegin;
		for (std::size_t pos = 1; _bindBegin != bindEnd && (*_bindBegin)->canBind(); ++_bindBegin)
		{
			if (boundRowCount != (*_bindBegin)->numOfRowsHandled())
				throw BindingException("Size mismatch in Bindings. All Bindings MUST have the same size");

			(*_bindBegin)->bind(pos);
			pos += (*_bindBegin)->numOfColumnsHandled();
		}

		if ((*oldBegin)->canBind())
		{
			//container binding will come back for more, so we must rewind
			_bindBegin = oldBegin;
			_canBind = true;
		}
		else _canBind = false;
	}
}


void SQLiteStatementImpl::clear()
{
	_columns[currentDataSet()].clear();
	_affectedRowCount = POCO_SQLITE_INV_ROW_CNT;

	if (_pStmt)
	{
		sqlite3_finalize(_pStmt);
		_pStmt=0;
	}
	_pLeftover = 0;
}


bool SQLiteStatementImpl::hasNext()
{
	if (_stepCalled)
		return (_nextResponse == SQLITE_ROW);

	// _pStmt is allowed to be null for conditional SQL statements
	if (_pStmt == 0)
	{
		_stepCalled   = true;
		_nextResponse = SQLITE_DONE;
		return false;
	}

	_stepCalled = true;
	_nextResponse = sqlite3_step(_pStmt);

	if (_affectedRowCount == POCO_SQLITE_INV_ROW_CNT) _affectedRowCount = 0;
	if (!sqlite3_stmt_readonly(_pStmt))
		_affectedRowCount += sqlite3_changes(_pDB);

	if (_nextResponse != SQLITE_ROW && _nextResponse != SQLITE_OK && _nextResponse != SQLITE_DONE)
		Utility::throwException(_nextResponse);

	_pExtractor->reset();//clear the cached null indicators

	return (_nextResponse == SQLITE_ROW);
}


std::size_t SQLiteStatementImpl::next()
{
	if (SQLITE_ROW == _nextResponse)
	{
		poco_assert (columnsReturned() == sqlite3_column_count(_pStmt));

		Extractions& extracts = extractions();
		Extractions::iterator it    = extracts.begin();
		Extractions::iterator itEnd = extracts.end();
		std::size_t pos = 0; // sqlite starts with pos 0 for results!
		for (; it != itEnd; ++it)
		{
			(*it)->extract(pos);
			pos += (*it)->numOfColumnsHandled();
			_isExtracted = true;
		}
		_stepCalled = false;
		if (_affectedRowCount == POCO_SQLITE_INV_ROW_CNT) _affectedRowCount = 0;
		_affectedRowCount += (*extracts.begin())->numOfRowsHandled();
	}
	else if (SQLITE_DONE == _nextResponse)
	{
		throw Poco::Data::DataException("No data received");
	}
	else
	{
		Utility::throwException(_nextResponse, std::string("Iterator Error: trying to access the next value"));
	}
	
	return 1u;
}


std::size_t SQLiteStatementImpl::columnsReturned() const
{
	return (std::size_t) _columns[currentDataSet()].size();
}


const MetaColumn& SQLiteStatementImpl::metaColumn(std::size_t pos) const
{
	std::size_t curDataSet = currentDataSet();
	poco_assert (pos >= 0 && pos <= _columns[curDataSet].size());
	return _columns[curDataSet][pos];
}


int SQLiteStatementImpl::affectedRowCount() const
{
	if (_affectedRowCount != POCO_SQLITE_INV_ROW_CNT) return _affectedRowCount;
	return _pStmt == 0 || sqlite3_stmt_readonly(_pStmt) ? 0 : sqlite3_changes(_pDB);
}


} } } // namespace Poco::Data::SQLite
