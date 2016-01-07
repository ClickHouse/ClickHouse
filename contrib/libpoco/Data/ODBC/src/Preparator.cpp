//
// Preparator.cpp
//
// $Id: //poco/Main/Data/ODBC/src/Preparator.cpp#5 $
//
// Library: Data
// Package: DataCore
// Module:  Preparator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/Preparator.h"
#include "Poco/Data/ODBC/ODBCMetaColumn.h"
#include "Poco/Exception.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Data {
namespace ODBC {


Preparator::Preparator(const StatementHandle& rStmt, 
	const std::string& statement, 
	std::size_t maxFieldSize,
	DataExtraction dataExtraction): 
	_rStmt(rStmt),
	_maxFieldSize(maxFieldSize),
	_dataExtraction(dataExtraction)
{
	SQLCHAR* pStr = (SQLCHAR*) statement.c_str();
	if (Utility::isError(Poco::Data::ODBC::SQLPrepare(_rStmt, pStr, (SQLINTEGER) statement.length())))
		throw StatementException(_rStmt);
}


Preparator::Preparator(const Preparator& other): 
	_rStmt(other._rStmt),
	_maxFieldSize(other._maxFieldSize),
	_dataExtraction(other._dataExtraction)
{
	resize();
}


Preparator::~Preparator()
{
	try
	{
		freeMemory();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void Preparator::freeMemory() const
{
	IndexMap::iterator it = _varLengthArrays.begin();
	IndexMap::iterator end = _varLengthArrays.end();
	for (; it != end; ++it)
	{
		switch (it->second)
		{
			case DT_BOOL:
				deleteCachedArray<bool>(it->first);
				break;

			case DT_CHAR:
				deleteCachedArray<char>(it->first);
				break;

			case DT_WCHAR:
				deleteCachedArray<UTF16String>(it->first);
				break;

			case DT_UCHAR:
				deleteCachedArray<unsigned char>(it->first);
				break;

			case DT_CHAR_ARRAY:
			{
				char** pc = AnyCast<char*>(&_values[it->first]);
				if (pc) std::free(*pc);
				break;
			}

			case DT_WCHAR_ARRAY:
			{
				UTF16String::value_type** pc = AnyCast<UTF16String::value_type*>(&_values[it->first]);
				if (pc) std::free(*pc);
				break;
			}

			case DT_UCHAR_ARRAY:
			{
				unsigned char** pc = AnyCast<unsigned char*>(&_values[it->first]);
				if (pc) std::free(*pc);
				break;
			}

			case DT_BOOL_ARRAY:
			{
				bool** pb = AnyCast<bool*>(&_values[it->first]);
				if (pb) std::free(*pb);
				break;
			}

			default:
				throw InvalidArgumentException("Unknown data type.");
		}
	}
}


std::size_t Preparator::columns() const
{
	if (_values.empty()) resize();
	return _values.size();
}


void Preparator::resize() const
{
	SQLSMALLINT nCol = 0;
	if (!Utility::isError(SQLNumResultCols(_rStmt, &nCol)) && 0 != nCol)
	{
		_values.resize(nCol, 0);
		_lengths.resize(nCol, 0);
		_lenLengths.resize(nCol);
		if(_varLengthArrays.size())
		{
			freeMemory();
			_varLengthArrays.clear();
		}
	}
}


std::size_t Preparator::maxDataSize(std::size_t pos) const
{
	poco_assert_dbg (pos < _values.size());

	std::size_t sz = 0;
	std::size_t maxsz = getMaxFieldSize();

	try 
	{
		ODBCMetaColumn mc(_rStmt, pos);
		sz = mc.length();

		// accomodate for terminating zero (non-bulk only!)
		MetaColumn::ColumnDataType type = mc.type();
		if (!isBulk() && ((ODBCMetaColumn::FDT_WSTRING == type) || (ODBCMetaColumn::FDT_STRING == type))) ++sz;
	}
	catch (StatementException&) { }

	if (!sz || sz > maxsz) sz = maxsz;

	return sz;
}


std::size_t Preparator::actualDataSize(std::size_t col, std::size_t row) const
{
	SQLLEN size = (POCO_DATA_INVALID_ROW == row) ? _lengths.at(col) :
		_lenLengths.at(col).at(row);

	// workaround for drivers returning negative length
	if (size < 0 && SQL_NULL_DATA != size) size *= -1;

	return size;
}


void Preparator::prepareBoolArray(std::size_t pos, SQLSMALLINT valueType, std::size_t length)
{
	poco_assert_dbg (DE_BOUND == _dataExtraction);
	poco_assert_dbg (pos < _values.size());
	poco_assert_dbg (pos < _lengths.size());
	poco_assert_dbg (pos < _lenLengths.size());

	bool* pArray = (bool*) std::calloc(length, sizeof(bool));

	_values[pos] = Any(pArray);
	_lengths[pos] = 0;
	_lenLengths[pos].resize(length);
	_varLengthArrays.insert(IndexMap::value_type(pos, DT_BOOL_ARRAY));

	if (Utility::isError(SQLBindCol(_rStmt, 
		(SQLUSMALLINT) pos + 1, 
		valueType, 
		(SQLPOINTER) pArray, 
		(SQLINTEGER) sizeof(bool), 
		&_lenLengths[pos][0])))
	{
		throw StatementException(_rStmt, "SQLBindCol()");
	}
}


} } } // namespace Poco::Data::ODBC
