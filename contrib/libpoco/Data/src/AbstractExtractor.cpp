//
// AbstractExtractor.cpp
//
// $Id: //poco/Main/Data/src/AbstractExtractor.cpp#2 $
//
// Library: Data
// Package: DataCore
// Module:  AbstractExtractor
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/AbstractExtractor.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Data {


AbstractExtractor::AbstractExtractor()
{
}


AbstractExtractor::~AbstractExtractor()
{
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::Int8>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::Int8>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::Int8>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::UInt8>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::UInt8>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::UInt8>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::Int16>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::Int16>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::Int16>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::UInt16>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::UInt16>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::UInt16>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::Int32>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::Int32>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::Int32>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::UInt32>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::UInt32>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::UInt32>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::Int64>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::Int64>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::Int64>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::UInt64>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::UInt64>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::UInt64>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


#ifndef POCO_LONG_IS_64_BIT
bool AbstractExtractor::extract(std::size_t pos, std::vector<long>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<long>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<long>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}
#endif


bool AbstractExtractor::extract(std::size_t pos, std::vector<bool>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<bool>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<bool>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<float>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<float>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<float>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<double>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<double>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<double>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<char>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<char>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<char>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<std::string>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<std::string>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<std::string>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, UTF16String& val)
{
	throw NotImplementedException("UTF16String extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<UTF16String>& val)
{
	throw NotImplementedException("std::vector<UTF16String> extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<UTF16String>& val)
{
	throw NotImplementedException("std::deque<UTF16String> extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<UTF16String>& val)
{
	throw NotImplementedException("std::list<UTF16String> extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<BLOB>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<BLOB>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<BLOB>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CLOB>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CLOB>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CLOB>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<DateTime>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<DateTime>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<DateTime>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Date>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Date>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Date>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Time>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Time>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Time>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Any>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Any>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Any>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Poco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Poco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Poco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


} } // namespace Poco::Data
