//
// Binder.cpp
//
// $Id: //poco/Main/Data/SQLite/src/Binder.cpp#5 $
//
// Library: SQLite
// Package: SQLite
// Module:  Binder
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLite/Binder.h"
#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Exception.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include <cstdlib>


using Poco::DateTimeFormatter;
using Poco::DateTimeFormat;


namespace Poco {
namespace Data {
namespace SQLite {


Binder::Binder(sqlite3_stmt* pStmt):
	_pStmt(pStmt)
{
}


Binder::~Binder()
{
}


void Binder::bind(std::size_t pos, const Poco::Int32 &val, Direction dir)
{
	int rc = sqlite3_bind_int(_pStmt, (int) pos, val);
	checkReturn(rc);
}


void Binder::bind(std::size_t pos, const Poco::Int64 &val, Direction dir)
{
	int rc = sqlite3_bind_int64(_pStmt, (int) pos, val);
	checkReturn(rc);
}


#ifndef POCO_LONG_IS_64_BIT
void Binder::bind(std::size_t pos, const long &val, Direction dir)
{
	long tmp = static_cast<long>(val);
	int rc = sqlite3_bind_int(_pStmt, (int) pos, tmp);
	checkReturn(rc);
}

void Binder::bind(std::size_t pos, const unsigned long &val, Direction dir)
{
	long tmp = static_cast<long>(val);
	int rc = sqlite3_bind_int(_pStmt, (int) pos, tmp);
	checkReturn(rc);
}
#endif


void Binder::bind(std::size_t pos, const double &val, Direction dir)
{
	int rc = sqlite3_bind_double(_pStmt, (int) pos, val);
	checkReturn(rc);
}


void Binder::bind(std::size_t pos, const std::string& val, Direction dir)
{
	int rc = sqlite3_bind_text(_pStmt, (int) pos, val.c_str(), (int) val.size()*sizeof(char), SQLITE_TRANSIENT);
	checkReturn(rc);
}


void Binder::bind(std::size_t pos, const Date& val, Direction dir)
{
	DateTime dt(val.year(), val.month(), val.day());
	std::string str(DateTimeFormatter::format(dt, Utility::SQLITE_DATE_FORMAT));
	bind(pos, str, dir);
}


void Binder::bind(std::size_t pos, const Time& val, Direction dir)
{
	DateTime dt;
	dt.assign(dt.year(), dt.month(), dt.day(), val.hour(), val.minute(), val.second());
	std::string str(DateTimeFormatter::format(dt, Utility::SQLITE_TIME_FORMAT));
	bind(pos, str, dir);
}


void Binder::bind(std::size_t pos, const DateTime& val, Direction dir)
{
	std::string dt(DateTimeFormatter::format(val, DateTimeFormat::ISO8601_FORMAT));
	bind(pos, dt, dir);
}


void Binder::bind(std::size_t pos, const NullData&, Direction)
{
	sqlite3_bind_null(_pStmt, pos);
}


void Binder::checkReturn(int rc)
{
	if (rc != SQLITE_OK)
		Utility::throwException(rc);
}


} } } // namespace Poco::Data::SQLite
