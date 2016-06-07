//
// Extractor.cpp
//
// $Id: //poco/Main/Data/SQLite/src/Extractor.cpp#5 $
//
// Library: SQLite
// Package: SQLite
// Module:  Extractor
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLite/Extractor.h"
#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/DataException.h"
#include "Poco/DateTimeParser.h"
#include "Poco/Exception.h"
#if defined(POCO_UNBUNDLED)
#include <sqlite3.h>
#else
#include "sqlite3.h"
#endif
#include <cstdlib>


using Poco::DateTimeParser;


namespace Poco {
namespace Data {
namespace SQLite {


Extractor::Extractor(sqlite3_stmt* pStmt):
	_pStmt(pStmt)
{
}


Extractor::~Extractor()
{
}


bool Extractor::extract(std::size_t pos, Poco::Int32& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::Int64& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int64(_pStmt, (int) pos);
	return true;
}


#ifndef POCO_LONG_IS_64_BIT
bool Extractor::extract(std::size_t pos, long& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, unsigned long& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}
#endif


bool Extractor::extract(std::size_t pos, double& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_double(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, std::string& val)
{
	if (isNull(pos)) return false;
	const char *pBuf = reinterpret_cast<const char*>(sqlite3_column_text(_pStmt, (int) pos));
	if (!pBuf)
		val.clear();
	else
		val.assign(pBuf);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::Int8& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::UInt8& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::Int16& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::UInt16& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::UInt32& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::UInt64& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int64(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, bool& val)
{
	if (isNull(pos)) return false;
	val = (0 != sqlite3_column_int(_pStmt, (int) pos));
	return true;
}


bool Extractor::extract(std::size_t pos, float& val)
{
	if (isNull(pos)) return false;
	val = static_cast<float>(sqlite3_column_double(_pStmt, (int) pos));
	return true;
}


bool Extractor::extract(std::size_t pos, char& val)
{
	if (isNull(pos)) return false;
	val = sqlite3_column_int(_pStmt, (int) pos);
	return true;
}


bool Extractor::extract(std::size_t pos, Date& val)
{
	if (isNull(pos)) return false;
	std::string str;
	extract(pos, str);
	int tzd;
	DateTime dt = DateTimeParser::parse(Utility::SQLITE_DATE_FORMAT, str, tzd);
	val = dt;
	return true;
}


bool Extractor::extract(std::size_t pos, Time& val)
{
	if (isNull(pos)) return false;
	std::string str;
	extract(pos, str);
	int tzd;
	DateTime dt = DateTimeParser::parse(Utility::SQLITE_TIME_FORMAT, str, tzd);
	val = dt;
	return true;
}


bool Extractor::extract(std::size_t pos, DateTime& val)
{
	if (isNull(pos)) return false;
	std::string dt;
	extract(pos, dt);
	int tzd;
	DateTimeParser::parse(dt, val, tzd);
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::Any& val)
{
	return extractImpl(pos, val);
}


bool Extractor::extract(std::size_t pos, Poco::DynamicAny& val)
{
	return extractImpl(pos, val);
}


bool Extractor::isNull(std::size_t pos, std::size_t)
{
	if (pos >= _nulls.size())
		_nulls.resize(pos + 1);

	if (!_nulls[pos].first)
	{
		_nulls[pos].first = true;
		_nulls[pos].second = (SQLITE_NULL == sqlite3_column_type(_pStmt, pos));
	}
	
	return _nulls[pos].second;
}


} } } // namespace Poco::Data::SQLite
