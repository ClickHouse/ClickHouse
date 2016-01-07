//
// AbstractPreparator.h
//
// $Id: //poco/Main/Data/include/Poco/Data/AbstractPreparator.h#5 $
//
// Library: Data
// Package: DataCore
// Module:  AbstractPreparator
//
// Definition of the AbstractPreparator class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_AbstractPreparator_INCLUDED
#define Data_AbstractPreparator_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/RefCountedObject.h"
#include "Poco/Data/LOB.h"
#include "Poco/UTFString.h"
#include <vector>
#include <deque>
#include <list>
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


class Data_API AbstractPreparator
	/// Interface used for database preparation where we first have to register all data types
	/// (and memory output locations) before extracting data, e.g. ODBC.
	/// Extract works as two-phase extract: first we call prepare once, then extract n-times.
	/// There are cases (bulk operations using std::vector storage) when extract is called only once.
	/// The value passed to a prepare() call is not used by the prepare, serving only as an indication 
	/// of the data type being prepared, thus all values are passed as const references. 
	/// Implementing this interface is not mandatory for a connector. Connectors that only extract data 
	/// after SQL execution (e.g. SQLite) do not need this functionality at all.
{
public:
	typedef SharedPtr<AbstractPreparator> Ptr;

	AbstractPreparator(Poco::UInt32 length = 1u);
		/// Creates the AbstractPreparator.

	virtual ~AbstractPreparator();
		/// Destroys the AbstractPreparator.

	virtual void prepare(std::size_t pos, const Poco::Int8&) = 0;
		/// Prepares an Int8.

	virtual void prepare(std::size_t pos, const std::vector<Poco::Int8>& val);
		/// Prepares an Int8 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::Int8>& val);
		/// Prepares an Int8 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::Int8>& val);
		/// Prepares an Int8 list.

	virtual void prepare(std::size_t pos, const Poco::UInt8&) = 0;
		/// Prepares an UInt8.

	virtual void prepare(std::size_t pos, const std::vector<Poco::UInt8>& val);
		/// Prepares an UInt8 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::UInt8>& val);
		/// Prepares an UInt8 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::UInt8>& val);
		/// Prepares an UInt8 list.

	virtual void prepare(std::size_t pos, const Poco::Int16&) = 0;
		/// Prepares an Int16.

	virtual void prepare(std::size_t pos, const std::vector<Poco::Int16>& val);
		/// Prepares an Int16 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::Int16>& val);
		/// Prepares an Int16 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::Int16>& val);
		/// Prepares an Int16 list.

	virtual void prepare(std::size_t pos, const Poco::UInt16&) = 0;
		/// Prepares an UInt16.

	virtual void prepare(std::size_t pos, const std::vector<Poco::UInt16>& val);
		/// Prepares an UInt16 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::UInt16>& val);
		/// Prepares an UInt16 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::UInt16>& val);
		/// Prepares an UInt16 list.

	virtual void prepare(std::size_t pos, const Poco::Int32&) = 0;
		/// Prepares an Int32.

	virtual void prepare(std::size_t pos, const std::vector<Poco::Int32>& val);
		/// Prepares an Int32 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::Int32>& val);
		/// Prepares an Int32 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::Int32>& val);
		/// Prepares an Int32 list.

	virtual void prepare(std::size_t pos, const Poco::UInt32&) = 0;
		/// Prepares an UInt32.

	virtual void prepare(std::size_t pos, const std::vector<Poco::UInt32>& val);
		/// Prepares an UInt32 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::UInt32>& val);
		/// Prepares an UInt32 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::UInt32>& val);
		/// Prepares an UInt32 list.

	virtual void prepare(std::size_t pos, const Poco::Int64&) = 0;
		/// Prepares an Int64.

	virtual void prepare(std::size_t pos, const std::vector<Poco::Int64>& val);
		/// Prepares an Int64 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::Int64>& val);
		/// Prepares an Int64 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::Int64>& val);
		/// Prepares an Int64 list.

	virtual void prepare(std::size_t pos, const Poco::UInt64&) = 0;
		/// Prepares an UInt64.

	virtual void prepare(std::size_t pos, const std::vector<Poco::UInt64>& val);
		/// Prepares an UInt64 vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::UInt64>& val);
		/// Prepares an UInt64 deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::UInt64>& val);
		/// Prepares an UInt64 list.

#ifndef POCO_LONG_IS_64_BIT
	virtual void prepare(std::size_t pos, const long&) = 0;
		/// Prepares a long.

	virtual void prepare(std::size_t pos, const unsigned long&) = 0;
		/// Prepares an unsigned long.

	virtual void prepare(std::size_t pos, const std::vector<long>& val);
		/// Prepares a long vector.

	virtual void prepare(std::size_t pos, const std::deque<long>& val);
		/// Prepares a long deque.

	virtual void prepare(std::size_t pos, const std::list<long>& val);
		/// Prepares a long list.
#endif

	virtual void prepare(std::size_t pos, const bool&) = 0;
		/// Prepares a boolean.

	virtual void prepare(std::size_t pos, const std::vector<bool>& val);
		/// Prepares a boolean vector.

	virtual void prepare(std::size_t pos, const std::deque<bool>& val);
		/// Prepares a boolean deque.

	virtual void prepare(std::size_t pos, const std::list<bool>& val);
		/// Prepares a boolean list.

	virtual void prepare(std::size_t pos, const float&) = 0;
		/// Prepares a float.

	virtual void prepare(std::size_t pos, const std::vector<float>& val);
		/// Prepares a float vector.

	virtual void prepare(std::size_t pos, const std::deque<float>& val);
		/// Prepares a float deque.

	virtual void prepare(std::size_t pos, const std::list<float>& val);
		/// Prepares a float list.

	virtual void prepare(std::size_t pos, const double&) = 0;
		/// Prepares a double.

	virtual void prepare(std::size_t pos, const std::vector<double>& val);
		/// Prepares a double vector.

	virtual void prepare(std::size_t pos, const std::deque<double>& val);
		/// Prepares a double deque.

	virtual void prepare(std::size_t pos, const std::list<double>& val);
		/// Prepares a double list.

	virtual void prepare(std::size_t pos, const char&) = 0;
		/// Prepares a single character.

	virtual void prepare(std::size_t pos, const std::vector<char>& val);
		/// Prepares a character vector.

	virtual void prepare(std::size_t pos, const std::deque<char>& val);
		/// Prepares a character deque.

	virtual void prepare(std::size_t pos, const std::list<char>& val);
		/// Prepares a character list.

	virtual void prepare(std::size_t pos, const std::string&) = 0;
		/// Prepares a string.

	virtual void prepare(std::size_t pos, const std::vector<std::string>& val);
		/// Prepares a string vector.

	virtual void prepare(std::size_t pos, const std::deque<std::string>& val);
		/// Prepares a string deque.

	virtual void prepare(std::size_t pos, const std::list<std::string>& val);
		/// Prepares a character list.

	virtual void prepare(std::size_t pos, const UTF16String&);
		/// Prepares a UTF16String.

	virtual void prepare(std::size_t pos, const std::vector<UTF16String>& val);
		/// Prepares a UTF16String vector.

	virtual void prepare(std::size_t pos, const std::deque<UTF16String>& val);
		/// Prepares a UTF16String deque.

	virtual void prepare(std::size_t pos, const std::list<UTF16String>& val);
		/// Prepares a UTF16String list.

	virtual void prepare(std::size_t pos, const BLOB&) = 0;
		/// Prepares a BLOB.

	virtual void prepare(std::size_t pos, const CLOB&) = 0;
		/// Prepares a CLOB.

	virtual void prepare(std::size_t pos, const std::vector<BLOB>& val);
		/// Prepares a BLOB vector.

	virtual void prepare(std::size_t pos, const std::deque<BLOB>& val);
		/// Prepares a BLOB deque.

	virtual void prepare(std::size_t pos, const std::list<BLOB>& val);
		/// Prepares a BLOB list.

	virtual void prepare(std::size_t pos, const std::vector<CLOB>& val);
		/// Prepares a CLOB vector.

	virtual void prepare(std::size_t pos, const std::deque<CLOB>& val);
		/// Prepares a CLOB deque.

	virtual void prepare(std::size_t pos, const std::list<CLOB>& val);
		/// Prepares a CLOB list.

	virtual void prepare(std::size_t pos, const DateTime&) = 0;
		/// Prepares a DateTime.

	virtual void prepare(std::size_t pos, const std::vector<DateTime>& val);
		/// Prepares a DateTime vector.

	virtual void prepare(std::size_t pos, const std::deque<DateTime>& val);
		/// Prepares a DateTime deque.

	virtual void prepare(std::size_t pos, const std::list<DateTime>& val);
		/// Prepares a DateTime list.

	virtual void prepare(std::size_t pos, const Date&) = 0;
		/// Prepares a Date.

	virtual void prepare(std::size_t pos, const std::vector<Date>& val);
		/// Prepares a Date vector.

	virtual void prepare(std::size_t pos, const std::deque<Date>& val);
		/// Prepares a Date deque.

	virtual void prepare(std::size_t pos, const std::list<Date>& val);
		/// Prepares a Date list.

	virtual void prepare(std::size_t pos, const Time&) = 0;
		/// Prepares a Time.

	virtual void prepare(std::size_t pos, const std::vector<Time>& val);
		/// Prepares a Time vector.

	virtual void prepare(std::size_t pos, const std::deque<Time>& val);
		/// Prepares a Time deque.
	
	virtual void prepare(std::size_t pos, const std::list<Time>& val);
		/// Prepares a Time list.

	virtual void prepare(std::size_t pos, const Any&) = 0;
		/// Prepares an Any.

	virtual void prepare(std::size_t pos, const std::vector<Any>& val);
		/// Prepares an Any vector.

	virtual void prepare(std::size_t pos, const std::deque<Any>& val);
		/// Prepares an Any deque.

	virtual void prepare(std::size_t pos, const std::list<Any>& val);
		/// Prepares an Any list.

	virtual void prepare(std::size_t pos, const Poco::Dynamic::Var&) = 0;
		/// Prepares a Var.

	virtual void prepare(std::size_t pos, const std::vector<Poco::Dynamic::Var>& val);
		/// Prepares a Var vector.

	virtual void prepare(std::size_t pos, const std::deque<Poco::Dynamic::Var>& val);
		/// Prepares a Var deque.

	virtual void prepare(std::size_t pos, const std::list<Poco::Dynamic::Var>& val);
		/// Prepares a Var list.

	void setLength(Poco::UInt32 length);
		/// Sets the length of prepared data.
		/// Needed only for data lengths greater than 1 (i.e. for
		/// bulk operations).

	Poco::UInt32 getLength() const;
		/// Returns the length of prepared data. Defaults to 1.
		/// The length is greater than one for bulk operations.

	void setBulk(bool bulkPrep = true);
		/// Sets bulk operation flag (always false at object creation time)

	bool isBulk() const;
		/// Returns bulk operation flag.

private:
	Poco::UInt32 _length;
	bool         _bulk;
};


///
/// inlines
///
inline void AbstractPreparator::setLength(Poco::UInt32 length)
{
	_length = length;
}


inline Poco::UInt32 AbstractPreparator::getLength() const
{
	return _length;
}


inline void AbstractPreparator::setBulk(bool bulkPrep)
{
	_bulk = bulkPrep;
}


inline bool AbstractPreparator::isBulk() const
{
	return _bulk;
}


} } // namespace Poco::Data


#endif // Data_AbstractPreparator_INCLUDED
