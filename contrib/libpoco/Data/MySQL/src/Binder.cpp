//
// MySQLException.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/Binder.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  Binder
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/Binder.h"


namespace Poco {
namespace Data {
namespace MySQL {


Binder::Binder()
{
}


Binder::~Binder()
{
	for (std::vector<MYSQL_TIME*>::iterator it = _dates.begin(); it != _dates.end(); ++it)
	{
		delete *it;
		*it = 0;
	}
}


void Binder::bind(std::size_t pos, const Poco::Int8& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_TINY, &val, 0);
}


void Binder::bind(std::size_t pos, const Poco::UInt8& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_TINY, &val, 0, true);
}


void Binder::bind(std::size_t pos, const Poco::Int16& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_SHORT, &val, 0);
}


void Binder::bind(std::size_t pos, const Poco::UInt16& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_SHORT, &val, 0, true);
}


void Binder::bind(std::size_t pos, const Poco::Int32& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_LONG, &val, 0);
}


void Binder::bind(std::size_t pos, const Poco::UInt32& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_LONG, &val, 0, true);
}


void Binder::bind(std::size_t pos, const Poco::Int64& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_LONGLONG, &val, 0);
}


void Binder::bind(std::size_t pos, const Poco::UInt64& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_LONGLONG, &val, 0, true);
}


#ifndef POCO_LONG_IS_64_BIT

void Binder::bind(std::size_t pos, const long& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_LONG, &val, 0);
}


void Binder::bind(std::size_t pos, const unsigned long& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_LONG, &val, 0, true);
}

#endif // POCO_LONG_IS_64_BIT


void Binder::bind(std::size_t pos, const bool& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_TINY, &val, 0);
}

	
void Binder::bind(std::size_t pos, const float& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_FLOAT, &val, 0);
}


void Binder::bind(std::size_t pos, const double& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_DOUBLE, &val, 0);
}


void Binder::bind(std::size_t pos, const char& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_TINY, &val, 0);
}


void Binder::bind(std::size_t pos, const std::string& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_STRING, val.c_str(), static_cast<int>(val.length()));
}


void Binder::bind(std::size_t pos, const Poco::Data::BLOB& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_BLOB, val.rawContent(), static_cast<int>(val.size()));
}


void Binder::bind(std::size_t pos, const Poco::Data::CLOB& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_BLOB, val.rawContent(), static_cast<int>(val.size()));
}


void Binder::bind(std::size_t pos, const DateTime& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	MYSQL_TIME mt = {0};

	mt.year = val.year();
	mt.month = val.month();
	mt.day = val.day();
	mt.hour = val.hour();
	mt.minute = val.minute();
	mt.second = val.second();
	mt.second_part = val.millisecond();

	mt.time_type  = MYSQL_TIMESTAMP_DATETIME;

	_dates.push_back(new MYSQL_TIME(mt));

	realBind(pos, MYSQL_TYPE_DATETIME, _dates.back(), sizeof(MYSQL_TIME));
}


void Binder::bind(std::size_t pos, const Date& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	MYSQL_TIME mt = {0};

	mt.year  = val.year();
	mt.month = val.month();
	mt.day   = val.day();

	mt.time_type = MYSQL_TIMESTAMP_DATE;

	_dates.push_back(new MYSQL_TIME(mt));
	
	realBind(pos, MYSQL_TYPE_DATE, _dates.back(), sizeof(MYSQL_TIME));
}


void Binder::bind(std::size_t pos, const Time& val, Direction dir)
{
	poco_assert(dir == PD_IN);
	MYSQL_TIME mt = {0};

	mt.hour   = val.hour();
	mt.minute = val.minute();
	mt.second = val.second();

	mt.time_type = MYSQL_TIMESTAMP_TIME;
	
	_dates.push_back(new MYSQL_TIME(mt));
	
	realBind(pos, MYSQL_TYPE_TIME, _dates.back(), sizeof(MYSQL_TIME));
}


void Binder::bind(std::size_t pos, const NullData&, Direction dir)
{
	poco_assert(dir == PD_IN);
	realBind(pos, MYSQL_TYPE_NULL, 0, 0);
}


std::size_t Binder::size() const
{
	return static_cast<std::size_t>(_bindArray.size());
}


MYSQL_BIND* Binder::getBindArray() const
{
	if (_bindArray.size() == 0)
	{
		return 0;
	}

	return const_cast<MYSQL_BIND*>(&_bindArray[0]);
}


/*void Binder::updateDates()
{
	for (std::size_t i = 0; i < _dates.size(); i++)
	{
		switch (_dates[i].mt.time_type)
		{
		case MYSQL_TIMESTAMP_DATE:
			_dates[i].mt.year  = _dates[i].link.date->year();
			_dates[i].mt.month = _dates[i].link.date->month();
			_dates[i].mt.day   = _dates[i].link.date->day();
			break;
		case MYSQL_TIMESTAMP_DATETIME:
			_dates[i].mt.year		= _dates[i].link.dateTime->year();
			_dates[i].mt.month	   = _dates[i].link.dateTime->month();
			_dates[i].mt.day		 = _dates[i].link.dateTime->day();
			_dates[i].mt.hour		= _dates[i].link.dateTime->hour();
			_dates[i].mt.minute	  = _dates[i].link.dateTime->minute();
			_dates[i].mt.second	  = _dates[i].link.dateTime->second();
			_dates[i].mt.second_part = _dates[i].link.dateTime->millisecond();
			break;
		case MYSQL_TIMESTAMP_TIME:
			_dates[i].mt.hour   = _dates[i].link.time->hour();
			_dates[i].mt.minute = _dates[i].link.time->minute();
			_dates[i].mt.second = _dates[i].link.time->second();
			break;
		}
	}
}*/

///////////////////
//
// Private 
//
////////////////////

void Binder::realBind(std::size_t pos, enum_field_types type, const void* buffer, int length, bool isUnsigned)
{
	if (pos >= _bindArray.size())
	{
		std::size_t s = static_cast<std::size_t>(_bindArray.size());
		_bindArray.resize(pos + 1);

		std::memset(&_bindArray[s], 0, sizeof(MYSQL_BIND) * (_bindArray.size() - s));
	}

	MYSQL_BIND b = {0};

	b.buffer_type   = type;
	b.buffer  = const_cast<void*>(buffer);
	b.buffer_length = length;
	b.is_unsigned   = isUnsigned;

	_bindArray[pos] = b;
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Int8>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Int8>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Int8>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::UInt8>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::UInt8>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::UInt8>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Int16>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Int16>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Int16>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::UInt16>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::UInt16>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::UInt16>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Int32>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Int32>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Int32>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::UInt32>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::UInt32>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::UInt32>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Int64>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Int64>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Int64>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::UInt64>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::UInt64>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::UInt64>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<bool>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<bool>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<bool>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<float>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<float>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<float>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<double>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<double>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<double>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<char>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<char>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<char>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Data::BLOB>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Data::BLOB>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Data::BLOB>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Data::CLOB>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Data::CLOB>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Data::CLOB>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::DateTime>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::DateTime>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::DateTime>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Data::Date>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Data::Date>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Data::Date>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Data::Time>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Data::Time>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Data::Time>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<Poco::Data::NullData>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<Poco::Data::NullData>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<Poco::Data::NullData>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::vector<std::string>& val, Direction dir) 
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::deque<std::string>& val, Direction dir)
{
	throw NotImplementedException();
}


void Binder::bind(std::size_t pos, const std::list<std::string>& val, Direction dir) 
{
	throw NotImplementedException();
}


} } } // namespace Poco::Data::MySQL
