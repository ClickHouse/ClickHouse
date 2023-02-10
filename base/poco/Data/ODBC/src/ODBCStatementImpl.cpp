//
// ODBCStatementImpl.cpp
//
// Library: Data/ODBC
// Package: ODBC
// Module:  ODBCStatementImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/ODBCStatementImpl.h"
#include "Poco/Data/ODBC/ConnectionHandle.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/ODBCException.h"
#include "Poco/Data/AbstractPreparation.h"
#include "Poco/Exception.h"


#ifdef POCO_OS_FAMILY_WINDOWS
	#pragma warning(disable:4312)// 'type cast' : conversion from 'std::size_t' to 'SQLPOINTER' of greater size
#endif


using Poco::DataFormatException;


namespace Poco {
namespace Data {
namespace ODBC {


const std::string ODBCStatementImpl::INVALID_CURSOR_STATE = "24000";


ODBCStatementImpl::ODBCStatementImpl(SessionImpl& rSession):
	Poco::Data::StatementImpl(rSession),
	_rConnection(rSession.dbc()),
	_stmt(rSession.dbc()),
	_stepCalled(false),
	_nextResponse(0),
	_prepared(false),
	_affectedRowCount(0),
	_canCompile(true)
{
	int queryTimeout = rSession.queryTimeout();
	if (queryTimeout >= 0)
	{
		SQLULEN uqt = static_cast<SQLULEN>(queryTimeout);
		SQLSetStmtAttr(_stmt,
			SQL_ATTR_QUERY_TIMEOUT,
			(SQLPOINTER) uqt,
			0);
	}
}


ODBCStatementImpl::~ODBCStatementImpl()
{
	ColumnPtrVecVec::iterator it = _columnPtrs.begin();
	ColumnPtrVecVec::iterator end = _columnPtrs.end();
	for (; it != end; ++it)
	{
		ColumnPtrVec::iterator itC = it->begin();
		ColumnPtrVec::iterator endC = it->end();
		for (; itC != endC; ++itC) delete *itC;
	}
}


void ODBCStatementImpl::compileImpl()
{
	if (!_canCompile) return;

	_stepCalled = false;
	_nextResponse = 0;

	if (_preparations.size())
		PreparatorVec().swap(_preparations);

	addPreparator();

	Binder::ParameterBinding bind = session().getFeature("autoBind") ? 
		Binder::PB_IMMEDIATE : Binder::PB_AT_EXEC;

	TypeInfo* pDT = 0;
	try
	{
		Poco::Any dti = session().getProperty("dataTypeInfo");
		pDT = AnyCast<TypeInfo*>(dti);
	}
	catch (NotSupportedException&) 
	{
	}

	std::size_t maxFieldSize = AnyCast<std::size_t>(session().getProperty("maxFieldSize"));
	
	_pBinder = new Binder(_stmt, maxFieldSize, bind, pDT);
	
	makeInternalExtractors();
	doPrepare();

	 _canCompile = false;
}


void ODBCStatementImpl::makeInternalExtractors()
{
	if (hasData() && !extractions().size()) 
	{
		try
		{
			fillColumns();
		} 
		catch (DataFormatException&)
		{
			if (isStoredProcedure()) return;
			throw;
		}

		makeExtractors(columnsReturned());
		fixupExtraction();
	}
}


void ODBCStatementImpl::addPreparator()
{
	if (0 == _preparations.size())
	{
		std::string statement(toString());
		if (statement.empty())
			throw ODBCException("Empty statements are illegal");

		Preparator::DataExtraction ext = session().getFeature("autoExtract") ? 
			Preparator::DE_BOUND : Preparator::DE_MANUAL;

		std::size_t maxFieldSize = AnyCast<std::size_t>(session().getProperty("maxFieldSize"));

		_preparations.push_back(new Preparator(_stmt, statement, maxFieldSize, ext));
	}
	else
		_preparations.push_back(new Preparator(*_preparations[0]));

	_extractors.push_back(new Extractor(_stmt, _preparations.back()));
}


void ODBCStatementImpl::doPrepare()
{
	if (session().getFeature("autoExtract") && hasData())
	{
		std::size_t curDataSet = currentDataSet();
		poco_check_ptr (_preparations[curDataSet]);

		Extractions& extracts = extractions();
		Extractions::iterator it    = extracts.begin();
		Extractions::iterator itEnd = extracts.end();

		if (it != itEnd && (*it)->isBulk())
		{
			std::size_t limit = getExtractionLimit();
			if (limit == Limit::LIMIT_UNLIMITED) 
				throw InvalidArgumentException("Bulk operation not allowed without limit.");
			checkError(Poco::Data::ODBC::SQLSetStmtAttr(_stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) limit, 0),
					"SQLSetStmtAttr(SQL_ATTR_ROW_ARRAY_SIZE)");
		}

		AbstractPreparation::Ptr pAP = 0;
		Poco::Data::AbstractPreparator::Ptr pP = _preparations[curDataSet];
		for (std::size_t pos = 0; it != itEnd; ++it)
		{
			pAP = (*it)->createPreparation(pP, pos);
			pAP->prepare();
			pos += (*it)->numOfColumnsHandled();
		}

		_prepared = true;
	}
}


bool ODBCStatementImpl::canBind() const
{
	if (!bindings().empty())
		return (*bindings().begin())->canBind();

	return false;
}


void ODBCStatementImpl::doBind()
{
	this->clear();
	Bindings& binds = bindings();
	if (!binds.empty())
	{
		Bindings::iterator it    = binds.begin();
		Bindings::iterator itEnd = binds.end();

		if (it != itEnd && 0 == _affectedRowCount)
			_affectedRowCount = static_cast<std::size_t>((*it)->numOfRowsHandled());

		for (std::size_t pos = 0; it != itEnd && (*it)->canBind(); ++it)
		{
			(*it)->bind(pos);
			pos += (*it)->numOfColumnsHandled();
		}
	}
}


void ODBCStatementImpl::bindImpl()
{
	doBind();

	SQLRETURN rc = SQLExecute(_stmt);

	if (SQL_NEED_DATA == rc) putData();
	else checkError(rc, "SQLExecute()");

	_pBinder->synchronize();
}


void ODBCStatementImpl::putData()
{
	SQLPOINTER pParam = 0;
	SQLINTEGER dataSize = 0;
	SQLRETURN rc;

	while (SQL_NEED_DATA == (rc = SQLParamData(_stmt, &pParam)))
	{
		if (pParam)
		{
			dataSize = (SQLINTEGER) _pBinder->parameterSize(pParam);

			if (Utility::isError(SQLPutData(_stmt, pParam, dataSize))) 
				throw StatementException(_stmt, "SQLPutData()");
		}
		else // if pParam is null pointer, do a dummy call
		{
			char dummy = 0;
			if (Utility::isError(SQLPutData(_stmt, &dummy, 0))) 
				throw StatementException(_stmt, "SQLPutData()");
		}
	}

	checkError(rc, "SQLParamData()");
}


void ODBCStatementImpl::clear()
{
	SQLRETURN rc = SQLCloseCursor(_stmt);
	_stepCalled = false;
	_affectedRowCount = 0;

	if (Utility::isError(rc))
	{
		StatementError err(_stmt);
		bool ignoreError = false;

		const StatementDiagnostics& diagnostics = err.diagnostics();
		//ignore "Invalid cursor state" error
		//(returned by 3.x drivers when cursor is not opened)
		for (int i = 0; i < diagnostics.count(); ++i)
		{
			if ((ignoreError =
				(INVALID_CURSOR_STATE == std::string(diagnostics.sqlState(i)))))
			{
				break;
			}
		}

		if (!ignoreError)
			throw StatementException(_stmt, "SQLCloseCursor()");
	}
}


bool ODBCStatementImpl::hasNext()
{
	if (hasData())
	{
		if (!extractions().size())
			makeInternalExtractors();

		if (!_prepared) doPrepare();

		if (_stepCalled)
			return _stepCalled = nextRowReady();

		makeStep();

		if (!nextRowReady())
		{
			if (hasMoreDataSets()) activateNextDataSet();
			else return false;

			if (SQL_NO_DATA == SQLMoreResults(_stmt))
				return false;

			addPreparator();
			doPrepare();
			fixupExtraction();
			makeStep();
		}
		else if (Utility::isError(_nextResponse))
			checkError(_nextResponse, "SQLFetch()");

		return true;
	}

	return false;
}


void ODBCStatementImpl::makeStep()
{
	_extractors[currentDataSet()]->reset();
	_nextResponse = SQLFetch(_stmt);
	checkError(_nextResponse);
	_stepCalled = true;
}


std::size_t ODBCStatementImpl::next()
{
	std::size_t count = 0;

	if (nextRowReady())
	{
		Extractions& extracts = extractions();
		Extractions::iterator it    = extracts.begin();
		Extractions::iterator itEnd = extracts.end();
		std::size_t prevCount = 0;
		for (std::size_t pos = 0; it != itEnd; ++it)
		{
			count = (*it)->extract(pos);
			if (prevCount && count != prevCount)
				throw IllegalStateException("Different extraction counts");
			prevCount = count;
			pos += (*it)->numOfColumnsHandled();
		}
		_stepCalled = false;
	}
	else
	{
		throw StatementException(_stmt,
			std::string("Next row not available."));
	}

	return count;
}


std::string ODBCStatementImpl::nativeSQL()
{
	std::string statement = toString();

	SQLINTEGER length = (SQLINTEGER) statement.size() * 2;

	char* pNative = 0;
	SQLINTEGER retlen = length;
	do
	{
		delete [] pNative;
		pNative = new char[retlen];
		std::memset(pNative, 0, retlen);
		length = retlen;
		if (Utility::isError(SQLNativeSql(_rConnection,
			(SQLCHAR*) statement.c_str(),
			(SQLINTEGER) statement.size(),
			(SQLCHAR*) pNative,
			length,
			&retlen)))
		{
			delete [] pNative;
			throw ConnectionException(_rConnection, "SQLNativeSql()");
		}
		++retlen;//accomodate for terminating '\0'
	}while (retlen > length);

	std::string sql(pNative);
	delete [] pNative;
	return sql;
}


void ODBCStatementImpl::checkError(SQLRETURN rc, const std::string& msg)
{
	if (SQL_NO_DATA == rc) return;

	if (Utility::isError(rc))
	{
		std::ostringstream os;
		os << std::endl << "Requested SQL statement: " << toString() << std::endl; 	 
		os << "Native SQL statement: " << nativeSQL() << std::endl; 	 
		std::string str(msg); str += os.str();
		
		throw StatementException(_stmt, str);
	}
}


void ODBCStatementImpl::fillColumns()
{
	std::size_t colCount = columnsReturned();
	std::size_t curDataSet = currentDataSet();
	if (curDataSet >= _columnPtrs.size())
		_columnPtrs.resize(curDataSet + 1);

	for (int i = 0; i < colCount; ++i)
		_columnPtrs[curDataSet].push_back(new ODBCMetaColumn(_stmt, i));
}


bool ODBCStatementImpl::isStoredProcedure() const 	 
{ 	 
	std::string str = toString(); 	 
	if (trimInPlace(str).size() < 2) return false; 	 

	return ('{' == str[0] && '}' == str[str.size()-1]); 	 
}


const MetaColumn& ODBCStatementImpl::metaColumn(std::size_t pos) const
{
	std::size_t curDataSet = currentDataSet();
	poco_assert_dbg (curDataSet < _columnPtrs.size());

	std::size_t sz = _columnPtrs[curDataSet].size();

	if (0 == sz || pos > sz - 1)
		throw InvalidAccessException(format("Invalid column number: %u", pos));

	return *_columnPtrs[curDataSet][pos];
}


int ODBCStatementImpl::affectedRowCount() const
{
	if (0 == _affectedRowCount)
	{
		SQLLEN rows = 0;
		if (!Utility::isError(SQLRowCount(_stmt, &rows)))
			_affectedRowCount = static_cast<std::size_t>(rows);
	}

	return static_cast<int>(_affectedRowCount);
}


} } } // namespace Poco::Data::ODBC
