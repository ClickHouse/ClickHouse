//
// MySQLException.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/Extractor.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  Extractor
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/Extractor.h"

#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"

namespace Poco {
namespace Data {
namespace MySQL {


Extractor::Extractor(StatementExecutor& st, ResultMetadata& md): _stmt(st), _metadata(md)
{
}


Extractor::~Extractor()
{
}


bool Extractor::extract(std::size_t pos, Poco::Int8& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_TINY, &val);
}


bool Extractor::extract(std::size_t pos, Poco::UInt8& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_TINY, &val, true);
}


bool Extractor::extract(std::size_t pos, Poco::Int16& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_SHORT, &val);
}


bool Extractor::extract(std::size_t pos, Poco::UInt16& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_SHORT, &val, true);
}


bool Extractor::extract(std::size_t pos, Poco::Int32& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_LONG, &val);
}


bool Extractor::extract(std::size_t pos, Poco::UInt32& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_LONG, &val, true);
}


bool Extractor::extract(std::size_t pos, Poco::Int64& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_LONGLONG, &val);
}


bool Extractor::extract(std::size_t pos, Poco::UInt64& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_LONGLONG, &val, true);
}


#ifndef POCO_LONG_IS_64_BIT
bool Extractor::extract(std::size_t pos, long& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_LONG, &val);
}


bool Extractor::extract(std::size_t pos, unsigned long& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_LONG, &val, true);
}
#endif


bool Extractor::extract(std::size_t pos, bool& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_TINY, &val);
}


bool Extractor::extract(std::size_t pos, float& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_FLOAT, &val);
}

	
bool Extractor::extract(std::size_t pos, double& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_DOUBLE, &val);
}

	
bool Extractor::extract(std::size_t pos, char& val)
{
	return realExtractFixed(pos, MYSQL_TYPE_TINY, &val);
}

	
bool Extractor::extract(std::size_t pos, std::string& val)
{
	if (_metadata.columnsReturned() <= pos)
		throw MySQLException("Extractor: attempt to extract more parameters, than query result contain");
	
	if (_metadata.isNull(static_cast<Poco::UInt32>(pos)))
	return false;
	
	//mysql reports TEXT types as FDT_BLOB when being extracted
	MetaColumn::ColumnDataType columnType = _metadata.metaColumn(static_cast<Poco::UInt32>(pos)).type();
	if (columnType != Poco::Data::MetaColumn::FDT_STRING && columnType != Poco::Data::MetaColumn::FDT_BLOB)
		throw MySQLException("Extractor: not a string");
		
	val.assign(reinterpret_cast<const char*>(_metadata.rawData(pos)), _metadata.length(pos));
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::Data::BLOB& val)
{
	if (_metadata.columnsReturned() <= pos)
		throw MySQLException("Extractor: attempt to extract more parameters, than query result contain");
	
	if (_metadata.isNull(static_cast<Poco::UInt32>(pos)))
	return false;
	
	if (_metadata.metaColumn(static_cast<Poco::UInt32>(pos)).type() != Poco::Data::MetaColumn::FDT_BLOB)
		throw MySQLException("Extractor: not a blob");
	
	val.assignRaw(_metadata.rawData(pos), _metadata.length(pos));
	return true;
}


bool Extractor::extract(std::size_t pos, Poco::Data::CLOB& val)
{
	if (_metadata.columnsReturned() <= pos)
		throw MySQLException("Extractor: attempt to extract more parameters, than query result contain");
	
	if (_metadata.isNull(static_cast<Poco::UInt32>(pos)))
	return false;
	
	if (_metadata.metaColumn(static_cast<Poco::UInt32>(pos)).type() != Poco::Data::MetaColumn::FDT_BLOB)
		throw MySQLException("Extractor: not a blob");
	
	val.assignRaw(reinterpret_cast<const char*>(_metadata.rawData(pos)), _metadata.length(pos));
	return true;
}


bool Extractor::extract(std::size_t pos, DateTime& val)
{
	MYSQL_TIME mt = {0};

	if (!realExtractFixed(pos, MYSQL_TYPE_DATETIME, &mt))
		return false;

	val.assign(mt.year, mt.month, mt.day, mt.hour, mt.minute, mt.second, mt.second_part, 0);
	return true;
}


bool Extractor::extract(std::size_t pos, Date& val)
{
	MYSQL_TIME mt = {0};

	if (!realExtractFixed(pos, MYSQL_TYPE_DATE, &mt))
		return false;

	val.assign(mt.year, mt.month, mt.day);
	return true;
}


bool Extractor::extract(std::size_t pos, Time& val)
{
	MYSQL_TIME mt = {0};

	if (!realExtractFixed(pos, MYSQL_TYPE_TIME, &mt))
		return false;

	val.assign(mt.hour, mt.minute, mt.second);
	return true;
}


bool Extractor::extract(std::size_t pos, Any& val)
{
	return false;
}


bool Extractor::extract(std::size_t pos, Dynamic::Var& val)
{
	return false;
}


bool Extractor::isNull(std::size_t col, std::size_t row)
{
	poco_assert(row == POCO_DATA_INVALID_ROW);

	if (_metadata.columnsReturned() <= col)
		throw MySQLException("Extractor: attempt to extract more parameters, than query result contain");

	if (_metadata.isNull(static_cast<Poco::UInt32>(col)))
		return true;

	return false;
}

void Extractor::reset()
{
	AbstractExtractor::reset();
}


bool Extractor::realExtractFixed(std::size_t pos, enum_field_types type, void* buffer, bool isUnsigned)
{
	MYSQL_BIND bind = {0};
	my_bool isNull = 0;

	bind.is_null	   = &isNull;
	bind.buffer_type   = type;
	bind.buffer		= buffer;
	bind.is_unsigned   = isUnsigned;
	
	if (!_stmt.fetchColumn(pos, &bind))
		return false;

	return isNull == 0;
}


//////////////
// Not implemented
//////////////


bool Extractor::extract(std::size_t , std::vector<Poco::Int8>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::Int8>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::Int8>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}
	
	
bool Extractor::extract(std::size_t , std::vector<Poco::UInt8>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::UInt8>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::UInt8>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Poco::Int16>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::Int16>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::Int16>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Poco::UInt16>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::UInt16>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::UInt16>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Poco::Int32>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::Int32>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::Int32>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Poco::UInt32>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::UInt32>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::UInt32>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Poco::Int64>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::Int64>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::Int64>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Poco::UInt64>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Poco::UInt64>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Poco::UInt64>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


#ifndef POCO_LONG_IS_64_BIT
bool Extractor::extract(std::size_t , std::vector<long>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<long>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<long>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}
#endif


bool Extractor::extract(std::size_t , std::vector<bool>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<bool>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<bool>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<float>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<float>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<float>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<double>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<double>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<double>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<char>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<char>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<char>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<std::string>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<std::string>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<std::string>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<BLOB>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<BLOB>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<BLOB>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<CLOB>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<CLOB>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<CLOB>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<DateTime>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<DateTime>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<DateTime>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Date>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Date>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Date>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Time>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Time>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Time>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Any>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Any>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Any>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::vector<Dynamic::Var>& )
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::deque<Dynamic::Var>& )
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool Extractor::extract(std::size_t , std::list<Dynamic::Var>& )
{
	throw NotImplementedException("std::list extractor must be implemented.");
}



} } } // namespace Poco::Data::MySQL
