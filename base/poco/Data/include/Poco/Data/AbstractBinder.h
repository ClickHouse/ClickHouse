//
// AbstractBinder.h
//
// Library: Data
// Package: DataCore
// Module:  AbstractBinder
//
// Definition of the AbstractBinder class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_AbstractBinder_INCLUDED
#define Data_AbstractBinder_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Data/LOB.h"
#include "Poco/DateTime.h"
#include "Poco/Nullable.h"
#include "Poco/Any.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/UTFString.h"
#include <vector>
#include <deque>
#include <list>
#include <cstddef>


namespace Poco {
namespace Data {


typedef NullType NullData;


namespace Keywords {


static const NullData null = NULL_GENERIC;


} // namespace Keywords


class Data_API AbstractBinder
	/// Interface for Binding data types to placeholders.
{
public:
	typedef SharedPtr<AbstractBinder> Ptr;

	enum Direction
		/// Binding direction for a parameter.
	{
		PD_IN,
		PD_OUT,
		PD_IN_OUT
	};

	AbstractBinder();
		/// Creates the AbstractBinder.

	virtual ~AbstractBinder();
		/// Destroys the AbstractBinder.

	virtual void bind(std::size_t pos, const Poco::Int8& val, Direction dir = PD_IN) = 0;
		/// Binds an Int8.

	virtual void bind(std::size_t pos, const std::vector<Poco::Int8>& val, Direction dir = PD_IN);
		/// Binds an Int8 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::Int8>& val, Direction dir = PD_IN);
		/// Binds an Int8 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::Int8>& val, Direction dir = PD_IN);
		/// Binds an Int8 list.

	virtual void bind(std::size_t pos, const Poco::UInt8& val, Direction dir = PD_IN) = 0;
		/// Binds an UInt8.

	virtual void bind(std::size_t pos, const std::vector<Poco::UInt8>& val, Direction dir = PD_IN);
		/// Binds an UInt8 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::UInt8>& val, Direction dir = PD_IN);
		/// Binds an UInt8 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::UInt8>& val, Direction dir = PD_IN);
		/// Binds an UInt8 list.

	virtual void bind(std::size_t pos, const Poco::Int16& val, Direction dir = PD_IN) = 0;
		/// Binds an Int16.

	virtual void bind(std::size_t pos, const std::vector<Poco::Int16>& val, Direction dir = PD_IN);
		/// Binds an Int16 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::Int16>& val, Direction dir = PD_IN);
		/// Binds an Int16 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::Int16>& val, Direction dir = PD_IN);
		/// Binds an Int16 list.

	virtual void bind(std::size_t pos, const Poco::UInt16& val, Direction dir = PD_IN) = 0;
		/// Binds an UInt16.

	virtual void bind(std::size_t pos, const std::vector<Poco::UInt16>& val, Direction dir = PD_IN);
		/// Binds an UInt16 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::UInt16>& val, Direction dir = PD_IN);
		/// Binds an UInt16 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::UInt16>& val, Direction dir = PD_IN);
		/// Binds an UInt16 list.

	virtual void bind(std::size_t pos, const Poco::Int32& val, Direction dir = PD_IN) = 0;
		/// Binds an Int32.

	virtual void bind(std::size_t pos, const std::vector<Poco::Int32>& val, Direction dir = PD_IN);
		/// Binds an Int32 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::Int32>& val, Direction dir = PD_IN);
		/// Binds an Int32 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::Int32>& val, Direction dir = PD_IN);
		/// Binds an Int32 list.

	virtual void bind(std::size_t pos, const Poco::UInt32& val, Direction dir = PD_IN) = 0;
		/// Binds an UInt32.

	virtual void bind(std::size_t pos, const std::vector<Poco::UInt32>& val, Direction dir = PD_IN);
		/// Binds an UInt32 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::UInt32>& val, Direction dir = PD_IN);
		/// Binds an UInt32 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::UInt32>& val, Direction dir = PD_IN);
		/// Binds an UInt32 list.
		
	virtual void bind(std::size_t pos, const Poco::Int64& val, Direction dir = PD_IN) = 0;
		/// Binds an Int64.

	virtual void bind(std::size_t pos, const std::vector<Poco::Int64>& val, Direction dir = PD_IN);
		/// Binds an Int64 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::Int64>& val, Direction dir = PD_IN);
		/// Binds an Int64 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::Int64>& val, Direction dir = PD_IN);
		/// Binds an Int64 list.

	virtual void bind(std::size_t pos, const Poco::UInt64& val, Direction dir = PD_IN) = 0;
		/// Binds an UInt64.

	virtual void bind(std::size_t pos, const std::vector<Poco::UInt64>& val, Direction dir = PD_IN);
		/// Binds an UInt64 vector.

	virtual void bind(std::size_t pos, const std::deque<Poco::UInt64>& val, Direction dir = PD_IN);
		/// Binds an UInt64 deque.

	virtual void bind(std::size_t pos, const std::list<Poco::UInt64>& val, Direction dir = PD_IN);
		/// Binds an UInt64 list.

#ifndef POCO_LONG_IS_64_BIT
	virtual void bind(std::size_t pos, const long& val, Direction dir = PD_IN) = 0;
		/// Binds a long.

	virtual void bind(std::size_t pos, const unsigned long& val, Direction dir = PD_IN) = 0;
		/// Binds an unsiged long.

	virtual void bind(std::size_t pos, const std::vector<long>& val, Direction dir = PD_IN);
		/// Binds a long vector.

	virtual void bind(std::size_t pos, const std::deque<long>& val, Direction dir = PD_IN);
		/// Binds a long deque.

	virtual void bind(std::size_t pos, const std::list<long>& val, Direction dir = PD_IN);
		/// Binds a long list.
#endif

	virtual void bind(std::size_t pos, const bool& val, Direction dir = PD_IN) = 0;
		/// Binds a boolean.

	virtual void bind(std::size_t pos, const std::vector<bool>& val, Direction dir = PD_IN);
		/// Binds a boolean vector.

	virtual void bind(std::size_t pos, const std::deque<bool>& val, Direction dir = PD_IN);
		/// Binds a boolean deque.

	virtual void bind(std::size_t pos, const std::list<bool>& val, Direction dir = PD_IN);
		/// Binds a boolean list.

	virtual void bind(std::size_t pos, const float& val, Direction dir = PD_IN) = 0;
		/// Binds a float.

	virtual void bind(std::size_t pos, const std::vector<float>& val, Direction dir = PD_IN);
		/// Binds a float vector.

	virtual void bind(std::size_t pos, const std::deque<float>& val, Direction dir = PD_IN);
		/// Binds a float deque.

	virtual void bind(std::size_t pos, const std::list<float>& val, Direction dir = PD_IN);
		/// Binds a float list.

	virtual void bind(std::size_t pos, const double& val, Direction dir = PD_IN) = 0;
		/// Binds a double.

	virtual void bind(std::size_t pos, const std::vector<double>& val, Direction dir = PD_IN);
		/// Binds a double vector.

	virtual void bind(std::size_t pos, const std::deque<double>& val, Direction dir = PD_IN);
		/// Binds a double deque.

	virtual void bind(std::size_t pos, const std::list<double>& val, Direction dir = PD_IN);
		/// Binds a double list.

	virtual void bind(std::size_t pos, const char& val, Direction dir = PD_IN) = 0;
		/// Binds a single character.

	virtual void bind(std::size_t pos, const std::vector<char>& val, Direction dir = PD_IN);
		/// Binds a character vector.

	virtual void bind(std::size_t pos, const std::deque<char>& val, Direction dir = PD_IN);
		/// Binds a character deque.

	virtual void bind(std::size_t pos, const std::list<char>& val, Direction dir = PD_IN);
		/// Binds a character list.

	virtual void bind(std::size_t pos, const char* const& pVal, Direction dir = PD_IN) = 0;
		/// Binds a const char ptr.

	virtual void bind(std::size_t pos, const std::string& val, Direction dir = PD_IN) = 0;
		/// Binds a string.

	virtual void bind(std::size_t pos, const std::vector<std::string>& val, Direction dir = PD_IN);
		/// Binds a string vector.

	virtual void bind(std::size_t pos, const std::deque<std::string>& val, Direction dir = PD_IN);
		/// Binds a string deque.

	virtual void bind(std::size_t pos, const std::list<std::string>& val, Direction dir = PD_IN);
		/// Binds a string list.

	virtual void bind(std::size_t pos, const UTF16String& val, Direction dir = PD_IN);
		/// Binds a UTF-16 Unicode string.

	virtual void bind(std::size_t pos, const std::vector<UTF16String>& val, Direction dir = PD_IN);
		/// Binds a UTF-16 Unicode string vector.

	virtual void bind(std::size_t pos, const std::deque<UTF16String>& val, Direction dir = PD_IN);
		/// Binds a UTF-16 Unicode string deque.

	virtual void bind(std::size_t pos, const std::list<UTF16String>& val, Direction dir = PD_IN);
		/// Binds a UTF-16 Unicode string list.

	virtual void bind(std::size_t pos, const BLOB& val, Direction dir = PD_IN) = 0;
		/// Binds a BLOB.

	virtual void bind(std::size_t pos, const CLOB& val, Direction dir = PD_IN) = 0;
		/// Binds a CLOB.

	virtual void bind(std::size_t pos, const std::vector<BLOB>& val, Direction dir = PD_IN);
		/// Binds a BLOB vector.

	virtual void bind(std::size_t pos, const std::deque<BLOB>& val, Direction dir = PD_IN);
		/// Binds a BLOB deque.

	virtual void bind(std::size_t pos, const std::list<BLOB>& val, Direction dir = PD_IN);
		/// Binds a BLOB list.

	virtual void bind(std::size_t pos, const std::vector<CLOB>& val, Direction dir = PD_IN);
		/// Binds a CLOB vector.

	virtual void bind(std::size_t pos, const std::deque<CLOB>& val, Direction dir = PD_IN);
		/// Binds a CLOB deque.

	virtual void bind(std::size_t pos, const std::list<CLOB>& val, Direction dir = PD_IN);
		/// Binds a CLOB list.

	virtual void bind(std::size_t pos, const DateTime& val, Direction dir = PD_IN) = 0;
		/// Binds a DateTime.

	virtual void bind(std::size_t pos, const std::vector<DateTime>& val, Direction dir = PD_IN);
		/// Binds a DateTime vector.

	virtual void bind(std::size_t pos, const std::deque<DateTime>& val, Direction dir = PD_IN);
		/// Binds a DateTime deque.

	virtual void bind(std::size_t pos, const std::list<DateTime>& val, Direction dir = PD_IN);
		/// Binds a DateTime list.

	virtual void bind(std::size_t pos, const Date& val, Direction dir = PD_IN) = 0;
		/// Binds a Date.

	virtual void bind(std::size_t pos, const std::vector<Date>& val, Direction dir = PD_IN);
		/// Binds a Date vector.

	virtual void bind(std::size_t pos, const std::deque<Date>& val, Direction dir = PD_IN);
		/// Binds a Date deque.

	virtual void bind(std::size_t pos, const std::list<Date>& val, Direction dir = PD_IN);
		/// Binds a Date list.

	virtual void bind(std::size_t pos, const Time& val, Direction dir = PD_IN) = 0;
		/// Binds a Time.

	virtual void bind(std::size_t pos, const std::vector<Time>& val, Direction dir = PD_IN);
		/// Binds a Time vector.

	virtual void bind(std::size_t pos, const std::deque<Time>& val, Direction dir = PD_IN);
		/// Binds a Time deque.

	virtual void bind(std::size_t pos, const std::list<Time>& val, Direction dir = PD_IN);
		/// Binds a Time list.

	virtual void bind(std::size_t pos, const NullData& val, Direction dir = PD_IN) = 0;
		/// Binds a null.

	virtual void bind(std::size_t pos, const std::vector<NullData>& val, Direction dir = PD_IN);
		/// Binds a null vector.

	virtual void bind(std::size_t pos, const std::deque<NullData>& val, Direction dir = PD_IN);
		/// Binds a null deque.

	virtual void bind(std::size_t pos, const std::list<NullData>& val, Direction dir = PD_IN);
		/// Binds a null list.

	void bind(std::size_t pos, const Any& val, Direction dir = PD_IN);
		/// Binds an Any.
	
	void bind(std::size_t pos, const Poco::Dynamic::Var& val, Direction dir = PD_IN);
	/// Binds a Var.

	virtual void reset();
		/// Resets a binder. No-op by default. Implement for binders that cache data.

	static bool isOutBound(Direction dir);
		/// Returns true if direction is out bound;

	static bool isInBound(Direction dir);
		/// Returns true if direction is in bound;
};


//
// inlines
//
inline void AbstractBinder::reset()
{
	//no-op
}


inline bool AbstractBinder::isOutBound(Direction dir)
{
	return PD_OUT == dir || PD_IN_OUT == dir;
}


inline bool AbstractBinder::isInBound(Direction dir)
{
	return PD_IN == dir || PD_IN_OUT == dir;
}


} } // namespace Poco::Data


#endif // Data_AbstractBinder_INCLUDED
