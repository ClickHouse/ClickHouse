//
// Extractor.h
//
// Library: Data/SQLite
// Package: SQLite
// Module:  Extractor
//
// Definition of the Extractor class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_SQLite_Extractor_INCLUDED
#define Data_SQLite_Extractor_INCLUDED


#include "Poco/Data/SQLite/SQLite.h"
#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/AbstractExtractor.h"
#include "Poco/Data/MetaColumn.h"
#include "Poco/Data/DataException.h"
#include "Poco/Data/Constants.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/Any.h"
#include "Poco/DynamicAny.h"
#include "sqlite3.h"
#include <vector>
#include <utility>


namespace Poco {
namespace Data {
namespace SQLite {


class SQLite_API Extractor: public Poco::Data::AbstractExtractor
	/// Extracts and converts data values form the result row returned by SQLite.
	/// If NULL is received, the incoming val value is not changed and false is returned
{
public:
	typedef std::vector<std::pair<bool, bool> > NullIndVec;
		/// Type for null indicators container.

	Extractor(sqlite3_stmt* pStmt);
		/// Creates the Extractor.

	~Extractor();
		/// Destroys the Extractor.

	bool extract(std::size_t pos, Poco::Int8& val);
		/// Extracts an Int8.

	bool extract(std::size_t pos, Poco::UInt8& val);
		/// Extracts an UInt8.

	bool extract(std::size_t pos, Poco::Int16& val);
		/// Extracts an Int16.

	bool extract(std::size_t pos, Poco::UInt16& val);
		/// Extracts an UInt16.

	bool extract(std::size_t pos, Poco::Int32& val);
		/// Extracts an Int32.

	bool extract(std::size_t pos, Poco::UInt32& val);
		/// Extracts an UInt32.

	bool extract(std::size_t pos, Poco::Int64& val);
		/// Extracts an Int64.

	bool extract(std::size_t pos, Poco::UInt64& val);
		/// Extracts an UInt64.

#ifndef POCO_INT64_IS_LONG
	bool extract(std::size_t pos, long& val);
		/// Extracts a long.

	bool extract(std::size_t pos, unsigned long& val);
		/// Extracts an unsigned long.
#endif

	bool extract(std::size_t pos, bool& val);
		/// Extracts a boolean.

	bool extract(std::size_t pos, float& val);
		/// Extracts a float.

	bool extract(std::size_t pos, double& val);
		/// Extracts a double.

	bool extract(std::size_t pos, char& val);
		/// Extracts a single character.

	bool extract(std::size_t pos, std::string& val);
		/// Extracts a string.

	bool extract(std::size_t pos, Poco::Data::BLOB& val);
		/// Extracts a BLOB.

	bool extract(std::size_t pos, Poco::Data::CLOB& val);
		/// Extracts a CLOB.

	bool extract(std::size_t pos, Poco::Data::Date& val);
		/// Extracts a Date.

	bool extract(std::size_t pos, Poco::Data::Time& val);
		/// Extracts a Time.

	bool extract(std::size_t pos, Poco::DateTime& val);
		/// Extracts a DateTime.

	bool extract(std::size_t pos, Poco::UUID& val);
		/// Extracts a Time.

	bool extract(std::size_t pos, Poco::Any& val);
		/// Extracts an Any.

	bool extract(std::size_t pos, Poco::DynamicAny& val);
		/// Extracts a DynamicAny.

	bool isNull(std::size_t pos, std::size_t row = POCO_DATA_INVALID_ROW);
		/// Returns true if the current row value at pos column is null.
		/// Because of the loss of information about null-ness of the
		/// underlying database values due to the nature of SQLite engine,
		/// (once null value is converted to default value, SQLite API
		/// treats it  as non-null), a null indicator container member
		/// variable is used to cache the indicators of the underlying nulls
		/// thus rendering this function idempotent.
		/// The container is a vector of [bool, bool] pairs.
		/// The vector index corresponds to the column position, the first
		/// bool value in the pair is true if the null indicator has
		/// been set and the second bool value in the pair is true if
		/// the column is actually null.
		/// The row argument, needed for connectors with bulk capabilities,
		/// is ignored in this implementation.

	void reset();
		/// Clears the cached nulls indicator vector.

private:
	template <typename T>
	bool extractImpl(std::size_t pos, T& val)
		/// Utility function for extraction of Any and DynamicAny.
	{
		if (isNull(pos)) return false;

		bool ret = false;

		switch (Utility::getColumnType(_pStmt, pos))
		{
		case MetaColumn::FDT_BOOL:
		{
			bool i = false;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_INT8:
		{
			Poco::Int8 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_UINT8:
		{
			Poco::UInt8 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_INT16:
		{
			Poco::Int16 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_UINT16:
		{
			Poco::UInt16 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_INT32:
		{
			Poco::Int32 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_UINT32:
		{
			Poco::UInt32 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_INT64:
		{
			Poco::Int64 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_UINT64:
		{
			Poco::UInt64 i = 0;
			ret = extract(pos, i);
			val = i;
			break;
		}
		case MetaColumn::FDT_STRING:
		{
			std::string s;
			ret = extract(pos, s);
			val = s;
			break;
		}
		case MetaColumn::FDT_DOUBLE:
		{
			double d(0.0);
			ret = extract(pos, d);
			val = d;
			break;
		}
		case MetaColumn::FDT_FLOAT:
		{
			float f(0.0);
			ret = extract(pos, f);
			val = f;
			break;
		}
		case MetaColumn::FDT_BLOB:
		{
			BLOB b;
			ret = extract(pos, b);
			val = b;
			break;
		}
		case MetaColumn::FDT_DATE:
		{
			Date d;
			ret = extract(pos, d);
			val = d;
			break;
		}
		case MetaColumn::FDT_TIME:
		{
			Time t;
			ret = extract(pos, t);
			val = t;
			break;
		}
		case MetaColumn::FDT_TIMESTAMP:
		{
			DateTime dt;
			ret = extract(pos, dt);
			val = dt;
			break;
		}
		case MetaColumn::FDT_UUID:
		{
			UUID uuid;
			ret = extract(pos, uuid);
			val = uuid;
			break;
		}
		default:
			throw Poco::Data::UnknownTypeException("Unknown type during extraction");
		}

		return ret;
	}

	template <typename T>
	bool extractLOB(std::size_t pos, Poco::Data::LOB<T>& val)
	{
		if (isNull(pos)) return false;
		int size = sqlite3_column_bytes(_pStmt, (int) pos);
		const T* pTmp = reinterpret_cast<const T*>(sqlite3_column_blob(_pStmt, (int) pos));
		val = Poco::Data::LOB<T>(pTmp, size);
		return true;
	}

	sqlite3_stmt* _pStmt;
	NullIndVec    _nulls;
};


///
/// inlines
///
inline void Extractor::reset()
{
	_nulls.clear();
}


inline bool Extractor::extract(std::size_t pos, Poco::Data::BLOB& val)
{
	return extractLOB<Poco::Data::BLOB::ValueType>(pos, val);
}


inline bool Extractor::extract(std::size_t pos, Poco::Data::CLOB& val)
{
	return extractLOB<Poco::Data::CLOB::ValueType>(pos, val);
}


} } } // namespace Poco::Data::SQLite


#endif // Data_SQLite_Extractor_INCLUDED
