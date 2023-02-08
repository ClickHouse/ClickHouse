//
// Utility.h
//
// Library: Data/ODBC
// Package: ODBC
// Module:  Utility
//
// Definition of Utility.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Utility_INCLUDED
#define Data_ODBC_Utility_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/Data/ODBC/TypeInfo.h"
#include "Poco/Data/Date.h"
#include "Poco/Data/Time.h"
#include "Poco/DateTime.h"
#include <sstream>
#include <map>
#include <sqltypes.h>


namespace Poco {
namespace Data {
namespace ODBC {


class ODBC_API Utility
	/// Various utility functions
{
public:
	typedef std::map<std::string, std::string> DSNMap;
	typedef DSNMap DriverMap;

	static bool isError(SQLRETURN rc);
		/// Returns true if return code is error

	static DriverMap& drivers(DriverMap& driverMap);
		/// Returns driver-attributes map of available ODBC drivers.

	static DSNMap& dataSources(DSNMap& dsnMap);
		/// Returns DSN-description map of available ODBC data sources.

	template<typename MapType, typename KeyArgType, typename ValueArgType>
	static typename MapType::iterator mapInsert(MapType& m, const KeyArgType& k, const ValueArgType& v)
		/// Utility map "insert or replace" function (from S. Meyers: Effective STL, Item 24)
	{
		typename MapType::iterator lb = m.lower_bound(k);
		if (lb != m.end() && !(m.key_comp()(k, lb->first)))
		{
			lb->second = v;
			return lb;
		}
		else
		{
			typedef typename MapType::value_type MVT;
			return m.insert(lb, MVT(k,v));
		}
	}

	static int cDataType(int sqlDataType);
		/// Returns C data type corresponding to supplied SQL data type.

	static int sqlDataType(int cDataType);
		/// Returns SQL data type corresponding to supplied C data type.

	static void dateSync(Date& dt, const SQL_DATE_STRUCT& ts);
		/// Transfers data from ODBC SQL_DATE_STRUCT to Poco::DateTime.

	template <typename T, typename F>
	static void dateSync(T& d, const F& ds)
		/// Transfers data from ODBC SQL_DATE_STRUCT container to Poco::DateTime container.
	{
		std::size_t size = ds.size();
		if (d.size() != size) d.resize(size);
		typename T::iterator dIt = d.begin();
		typename F::const_iterator it = ds.begin();
		typename F::const_iterator end = ds.end();
		for (; it != end; ++it, ++dIt) dateSync(*dIt, *it);
	}

	static void timeSync(Time& dt, const SQL_TIME_STRUCT& ts);
		/// Transfers data from ODBC SQL_TIME_STRUCT to Poco::DateTime.

	template <typename T, typename F>
	static void timeSync(T& t, const F& ts)
		/// Transfers data from ODBC SQL_TIME_STRUCT container to Poco::DateTime container.
	{
		std::size_t size = ts.size();
		if (t.size() != size) t.resize(size);
		typename T::iterator dIt = t.begin();
		typename F::const_iterator it = ts.begin();
		typename F::const_iterator end = ts.end();
		for (; it != end; ++it, ++dIt) timeSync(*dIt, *it);
	}

	static void dateTimeSync(Poco::DateTime& dt, const SQL_TIMESTAMP_STRUCT& ts);
		/// Transfers data from ODBC SQL_TIMESTAMP_STRUCT to Poco::DateTime.

	template <typename T, typename F>
	static void dateTimeSync(T& dt, const F& ts)
		/// Transfers data from ODBC SQL_TIMESTAMP_STRUCT container to Poco::DateTime container.
	{
		std::size_t size = ts.size();
		if (dt.size() != size) dt.resize(size);
		typename T::iterator dIt = dt.begin();
		typename F::const_iterator it = ts.begin();
		typename F::const_iterator end = ts.end();
		for (; it != end; ++it, ++dIt) dateTimeSync(*dIt, *it);
	}

	static void dateSync(SQL_DATE_STRUCT& ts, const Date& dt);
		/// Transfers data from Poco::Data::Date to ODBC SQL_DATE_STRUCT.

	template <typename C>
	static void dateSync(std::vector<SQL_DATE_STRUCT>& ds, const C& d)
		/// Transfers data from Poco::Data::Date vector to ODBC SQL_DATE_STRUCT container.
	{
		std::size_t size = d.size();
		if (ds.size() != size) ds.resize(size);
		std::vector<SQL_DATE_STRUCT>::iterator dIt = ds.begin();
		typename C::const_iterator it = d.begin();
		typename C::const_iterator end = d.end();
		for (; it != end; ++it, ++dIt) dateSync(*dIt, *it);
	}

	static void timeSync(SQL_TIME_STRUCT& ts, const Time& dt);
		/// Transfers data from Poco::Data::Time to ODBC SQL_TIME_STRUCT.

	template <typename C>
	static void timeSync(std::vector<SQL_TIME_STRUCT>& ts, const C& t)
		/// Transfers data from Poco::Data::Time container to ODBC SQL_TIME_STRUCT vector.
	{
		std::size_t size = t.size();
		if (ts.size() != size) ts.resize(size);
		std::vector<SQL_TIME_STRUCT>::iterator tIt = ts.begin();
		typename C::const_iterator it = t.begin();
		typename C::const_iterator end = t.end();
		for (; it != end; ++it, ++tIt) timeSync(*tIt, *it);
	}

	static void dateTimeSync(SQL_TIMESTAMP_STRUCT& ts, const Poco::DateTime& dt);
		/// Transfers data from Poco::DateTime to ODBC SQL_TIMESTAMP_STRUCT.

	template <typename C>
	static void dateTimeSync(std::vector<SQL_TIMESTAMP_STRUCT>& ts, const C& dt)
		/// Transfers data from Poco::DateTime to ODBC SQL_TIMESTAMP_STRUCT.
	{
		std::size_t size = dt.size();
		if (ts.size() != size) ts.resize(size);
		std::vector<SQL_TIMESTAMP_STRUCT>::iterator tIt = ts.begin();
		typename C::const_iterator it = dt.begin();
		typename C::const_iterator end = dt.end();
		for (; it != end; ++it, ++tIt) dateTimeSync(*tIt, *it);
	}

private:
	static const TypeInfo _dataTypes;
		/// C <==> SQL data type mapping
};


///
/// inlines
///
inline bool Utility::isError(SQLRETURN rc)
{
	return (0 != (rc & (~1)));
}


inline int Utility::cDataType(int sqlDataType)
{
	return _dataTypes.cDataType(sqlDataType);
}


inline int Utility::sqlDataType(int cDataType)
{
	return _dataTypes.sqlDataType(cDataType);
}


inline void Utility::dateSync(Date& d, const SQL_DATE_STRUCT& ts)
{
	d.assign(ts.year, ts.month, ts.day);
}


inline void Utility::timeSync(Time& t, const SQL_TIME_STRUCT& ts)
{
	t.assign(ts.hour, ts.minute, ts.second);
}


} } } // namespace Poco::Data::ODBC


#endif
