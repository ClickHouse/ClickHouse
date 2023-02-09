//
// AbstractExtractor.h
//
// Library: Data
// Package: DataCore
// Module:  AbstractExtractor
//
// Definition of the AbstractExtractor class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_AbstractExtractor_INCLUDED
#define Data_AbstractExtractor_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/Constants.h"
#include "Poco/Data/LOB.h"
#include "Poco/UTFString.h"
#include <vector>
#include <deque>
#include <list>
#include <string>
#include <cstddef>


namespace Poco {


class DateTime;
class Any;

namespace Dynamic {
class Var;
}

namespace Data {


class Date;
class Time;


class Data_API AbstractExtractor
	/// Interface used to extract data from a single result row.
	/// If an extractor receives null it is not allowed to change val!
{
public:
	typedef SharedPtr<AbstractExtractor> Ptr;

	AbstractExtractor();
		/// Creates the AbstractExtractor.

	virtual ~AbstractExtractor();
		/// Destroys the AbstractExtractor.

	virtual bool extract(std::size_t pos, Poco::Int8& val) = 0;
		/// Extracts an Int8. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::Int8>& val);
		/// Extracts an Int8 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int8>& val);
		/// Extracts an Int8 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int8>& val);
		/// Extracts an Int8 list.

	virtual bool extract(std::size_t pos, Poco::UInt8& val) = 0;
		/// Extracts an UInt8. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt8>& val);
		/// Extracts an UInt8 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt8>& val);
		/// Extracts an UInt8 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt8>& val);
		/// Extracts an UInt8 list.

	virtual bool extract(std::size_t pos, Poco::Int16& val) = 0;
		/// Extracts an Int16. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::Int16>& val);
		/// Extracts an Int16 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int16>& val);
		/// Extracts an Int16 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int16>& val);
		/// Extracts an Int16 list.

	virtual bool extract(std::size_t pos, Poco::UInt16& val) = 0;
		/// Extracts an UInt16. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt16>& val);
		/// Extracts an UInt16 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt16>& val);
		/// Extracts an UInt16 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt16>& val);
		/// Extracts an UInt16 list.

	virtual bool extract(std::size_t pos, Poco::Int32& val) = 0;
		/// Extracts an Int32. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::Int32>& val);
		/// Extracts an Int32 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int32>& val);
		/// Extracts an Int32 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int32>& val);
		/// Extracts an Int32 list.

	virtual bool extract(std::size_t pos, Poco::UInt32& val) = 0;
		/// Extracts an UInt32. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt32>& val);
		/// Extracts an UInt32 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt32>& val);
		/// Extracts an UInt32 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt32>& val);
		/// Extracts an UInt32 list.

	virtual bool extract(std::size_t pos, Poco::Int64& val) = 0;
		/// Extracts an Int64. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::Int64>& val);
		/// Extracts an Int64 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int64>& val);
		/// Extracts an Int64 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int64>& val);
		/// Extracts an Int64 list.

	virtual bool extract(std::size_t pos, Poco::UInt64& val) = 0;
		/// Extracts an UInt64. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt64>& val);
		/// Extracts an UInt64 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt64>& val);
		/// Extracts an UInt64 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt64>& val);
		/// Extracts an UInt64 list.

#ifndef POCO_LONG_IS_64_BIT
	virtual bool extract(std::size_t pos, long& val) = 0;
		/// Extracts a long. Returns false if null was received.

	virtual bool extract(std::size_t pos, unsigned long& val) = 0;
		/// Extracts an unsigned long. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<long>& val);
		/// Extracts a long vector.

	virtual bool extract(std::size_t pos, std::deque<long>& val);
		/// Extracts a long deque.

	virtual bool extract(std::size_t pos, std::list<long>& val);
		/// Extracts a long list.
#endif

	virtual bool extract(std::size_t pos, bool& val) = 0;
		/// Extracts a boolean. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<bool>& val);
		/// Extracts a boolean vector.

	virtual bool extract(std::size_t pos, std::deque<bool>& val);
		/// Extracts a boolean deque.

	virtual bool extract(std::size_t pos, std::list<bool>& val);
		/// Extracts a boolean list.

	virtual bool extract(std::size_t pos, float& val) = 0;
		/// Extracts a float. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<float>& val);
		/// Extracts a float vector.

	virtual bool extract(std::size_t pos, std::deque<float>& val);
		/// Extracts a float deque.

	virtual bool extract(std::size_t pos, std::list<float>& val);
		/// Extracts a float list.

	virtual bool extract(std::size_t pos, double& val) = 0;
		/// Extracts a double. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<double>& val);
		/// Extracts a double vector.

	virtual bool extract(std::size_t pos, std::deque<double>& val);
		/// Extracts a double deque.

	virtual bool extract(std::size_t pos, std::list<double>& val);
		/// Extracts a double list.

	virtual bool extract(std::size_t pos, char& val) = 0;
		/// Extracts a single character. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<char>& val);
		/// Extracts a character vector.

	virtual bool extract(std::size_t pos, std::deque<char>& val);
		/// Extracts a character deque.

	virtual bool extract(std::size_t pos, std::list<char>& val);
		/// Extracts a character list.

	virtual bool extract(std::size_t pos, std::string& val) = 0;
		/// Extracts a string. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<std::string>& val);
		/// Extracts a string vector.

	virtual bool extract(std::size_t pos, std::deque<std::string>& val);
		/// Extracts a string deque.

	virtual bool extract(std::size_t pos, std::list<std::string>& val);
		/// Extracts a string list.

	virtual bool extract(std::size_t pos, UTF16String& val);
		/// Extracts a UTF16String. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<UTF16String>& val);
		/// Extracts a UTF16String vector.

	virtual bool extract(std::size_t pos, std::deque<UTF16String>& val);
		/// Extracts a UTF16String deque.

	virtual bool extract(std::size_t pos, std::list<UTF16String>& val);
		/// Extracts a UTF16String list.

	virtual bool extract(std::size_t pos, BLOB& val) = 0;
		/// Extracts a BLOB. Returns false if null was received.

	virtual bool extract(std::size_t pos, CLOB& val) = 0;
		/// Extracts a CLOB. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<BLOB>& val);
		/// Extracts a BLOB vector.

	virtual bool extract(std::size_t pos, std::deque<BLOB>& val);
		/// Extracts a BLOB deque.

	virtual bool extract(std::size_t pos, std::list<BLOB>& val);
		/// Extracts a BLOB list.

	virtual bool extract(std::size_t pos, std::vector<CLOB>& val);
		/// Extracts a CLOB vector.

	virtual bool extract(std::size_t pos, std::deque<CLOB>& val);
		/// Extracts a CLOB deque.

	virtual bool extract(std::size_t pos, std::list<CLOB>& val);
		/// Extracts a CLOB list.

	virtual bool extract(std::size_t pos, DateTime& val) = 0;
		/// Extracts a DateTime. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<DateTime>& val);
		/// Extracts a DateTime vector.

	virtual bool extract(std::size_t pos, std::deque<DateTime>& val);
		/// Extracts a DateTime deque.

	virtual bool extract(std::size_t pos, std::list<DateTime>& val);
		/// Extracts a DateTime list.

	virtual bool extract(std::size_t pos, Date& val) = 0;
		/// Extracts a Date. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Date>& val);
		/// Extracts a Date vector.

	virtual bool extract(std::size_t pos, std::deque<Date>& val);
		/// Extracts a Date deque.

	virtual bool extract(std::size_t pos, std::list<Date>& val);
		/// Extracts a Date list.

	virtual bool extract(std::size_t pos, Time& val) = 0;
		/// Extracts a Time. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Time>& val);
		/// Extracts a Time vector.

	virtual bool extract(std::size_t pos, std::deque<Time>& val);
		/// Extracts a Time deque.

	virtual bool extract(std::size_t pos, std::list<Time>& val);
		/// Extracts a Time list.

	virtual bool extract(std::size_t pos, Any& val) = 0;
		/// Extracts an Any. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Any>& val);
		/// Extracts an Any vector.

	virtual bool extract(std::size_t pos, std::deque<Any>& val);
		/// Extracts an Any deque.

	virtual bool extract(std::size_t pos, std::list<Any>& val);
		/// Extracts an Any list.

	virtual bool extract(std::size_t pos, Poco::Dynamic::Var& val) = 0;
		/// Extracts a Var. Returns false if null was received.

	virtual bool extract(std::size_t pos, std::vector<Poco::Dynamic::Var>& val);
		/// Extracts a Var vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Dynamic::Var>& val);
		/// Extracts a Var deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Dynamic::Var>& val);
		/// Extracts a Var list.

	virtual bool isNull(std::size_t col, std::size_t row = POCO_DATA_INVALID_ROW) = 0;
		/// Returns true if the value at [col,row] position is null.

	virtual void reset();
		/// Resets any information internally cached by the extractor.
};


///
/// inlines
///
inline void AbstractExtractor::reset()
{
	//default no-op
}


} } // namespace Poco::Data


#endif // Data_AbstractExtractor_INCLUDED
