//
// AbstractBinder.cpp
//
// $Id: //poco/Main/Data/src/AbstractBinder.cpp#4 $
//
// Library: Data
// Package: DataCore
// Module:  AbstractBinder
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/AbstractBinder.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/DataException.h"
#include "Poco/DateTime.h"
#include "Poco/Any.h"
#include "Poco/Dynamic/Var.h"


namespace Poco {
namespace Data {


AbstractBinder::AbstractBinder()
{
}


AbstractBinder::~AbstractBinder()
{
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::Int8>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::Int8>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Poco::Int8>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::UInt8>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::UInt8>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Poco::UInt8>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::Int16>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::Int16>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Poco::Int16>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::UInt16>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::UInt16>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Poco::UInt16>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::Int32>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::Int32>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Poco::Int32>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::UInt32>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::UInt32>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Poco::UInt32>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::Int64>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::Int64>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Poco::Int64>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}

	
void AbstractBinder::bind(std::size_t pos, const std::vector<Poco::UInt64>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}

	
void AbstractBinder::bind(std::size_t pos, const std::deque<Poco::UInt64>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}

	
void AbstractBinder::bind(std::size_t pos, const std::list<Poco::UInt64>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


#ifndef POCO_LONG_IS_64_BIT
void AbstractBinder::bind(std::size_t pos, const std::vector<long>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<long>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<long>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}
#endif


void AbstractBinder::bind(std::size_t pos, const std::vector<bool>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<bool>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<bool>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<float>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<float>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<float>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<double>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<double>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<double>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<char>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<char>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<char>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<std::string>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<std::string>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<std::string>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const UTF16String& val, Direction dir)
{
	throw NotImplementedException("UTF16String binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<UTF16String>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<UTF16String>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<UTF16String>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<BLOB>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<BLOB>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<BLOB>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<CLOB>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<CLOB>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<CLOB>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<DateTime>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<DateTime>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<DateTime>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Date>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Date>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Date>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<Time>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<Time>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<Time>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::vector<NullData>& val, Direction dir)
{
	throw NotImplementedException("std::vector binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::deque<NullData>& val, Direction dir)
{
	throw NotImplementedException("std::deque binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const std::list<NullData>& val, Direction dir)
{
	throw NotImplementedException("std::list binder must be implemented.");
}


void AbstractBinder::bind(std::size_t pos, const Any& val, Direction dir)
{
	const std::type_info& type = val.type();

	if(type == typeid(Int32))
		bind(pos, RefAnyCast<Int32>(val), dir);
	else if(type == typeid(std::string))
		bind(pos, RefAnyCast<std::string>(val), dir);
	else if (type == typeid(Poco::UTF16String))
		bind(pos, RefAnyCast<Poco::UTF16String>(val), dir);
	else if (type == typeid(bool))
		bind(pos, RefAnyCast<bool>(val), dir);
	else if(type == typeid(char))
		bind(pos, RefAnyCast<char>(val), dir);
	else if(type == typeid(Int8))
		bind(pos, RefAnyCast<Int8>(val), dir);
	else if(type == typeid(UInt8))
		bind(pos, RefAnyCast<UInt8>(val), dir);
	else if(type == typeid(Int16))
		bind(pos, RefAnyCast<Int16>(val), dir);
	else if(type == typeid(UInt16))
		bind(pos, RefAnyCast<UInt16>(val), dir);
	else if(type == typeid(UInt32))
		bind(pos, RefAnyCast<UInt32>(val), dir);
	else if(type == typeid(Int64))
		bind(pos, RefAnyCast<Int64>(val), dir);
	else if(type == typeid(UInt64))
		bind(pos, RefAnyCast<UInt64>(val), dir);
	else if(type == typeid(float))
		bind(pos, RefAnyCast<float>(val), dir);
	else if(type == typeid(double))
		bind(pos, RefAnyCast<double>(val), dir);
	else if(type == typeid(DateTime))
		bind(pos, RefAnyCast<DateTime>(val), dir);
	else if(type == typeid(Date))
		bind(pos, RefAnyCast<Date>(val), dir);
	else if(type == typeid(Time))
		bind(pos, RefAnyCast<Time>(val), dir);
	else if(type == typeid(BLOB))
		bind(pos, RefAnyCast<BLOB>(val), dir);
	else if(type == typeid(void))
		bind(pos, Keywords::null, dir);
#ifndef POCO_LONG_IS_64_BIT
	else if(type == typeid(long))
		bind(pos, RefAnyCast<long>(val), dir);
#endif
	else
		throw UnknownTypeException(std::string(val.type().name()));
}


void AbstractBinder::bind(std::size_t pos, const Poco::Dynamic::Var& val, Direction dir)
{
	const std::type_info& type = val.type();

	if(type == typeid(Int32))
		bind(pos, val.extract<Int32>(), dir);
	else if(type == typeid(std::string))
		bind(pos, val.extract<std::string>(), dir);
	else if (type == typeid(Poco::UTF16String))
		bind(pos, val.extract<Poco::UTF16String>(), dir);
	else if (type == typeid(bool))
		bind(pos, val.extract<bool>(), dir);
	else if(type == typeid(char))
		bind(pos, val.extract<char>(), dir);
	else if(type == typeid(Int8))
		bind(pos, val.extract<Int8>(), dir);
	else if(type == typeid(UInt8))
		bind(pos, val.extract<UInt8>(), dir);
	else if(type == typeid(Int16))
		bind(pos, val.extract<Int16>(), dir);
	else if(type == typeid(UInt16))
		bind(pos, val.extract<UInt16>(), dir);
	else if(type == typeid(UInt32))
		bind(pos, val.extract<UInt32>(), dir);
	else if(type == typeid(Int64))
		bind(pos, val.extract<Int64>(), dir);
	else if(type == typeid(UInt64))
		bind(pos, val.extract<UInt64>(), dir);
	else if(type == typeid(float))
		bind(pos, val.extract<float>(), dir);
	else if(type == typeid(double))
		bind(pos, val.extract<double>(), dir);
	else if(type == typeid(DateTime))
		bind(pos, val.extract<DateTime>(), dir);
	else if(type == typeid(Date))
		bind(pos, val.extract<Date>(), dir);
	else if(type == typeid(Time))
		bind(pos, val.extract<Time>(), dir);
	else if(type == typeid(BLOB))
		bind(pos, val.extract<BLOB>(), dir);
	else if(type == typeid(void))
		bind(pos, Keywords::null, dir);
#ifndef POCO_LONG_IS_64_BIT
	else if(type == typeid(long))
		bind(pos, val.extract<long>(), dir);
#endif
	else
		throw UnknownTypeException(std::string(val.type().name()));
}


} } // namespace Poco::Data
