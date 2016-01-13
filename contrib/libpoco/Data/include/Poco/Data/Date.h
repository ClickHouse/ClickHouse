//
// Date.h
//
// $Id: //poco/Main/Data/include/Poco/Data/Date.h#7 $
//
// Library: Data
// Package: DataCore
// Module:  Date
//
// Definition of the Date class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Date_INCLUDED
#define Data_Date_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Dynamic/VarHolder.h"
#include "Poco/Exception.h"


namespace Poco {

class DateTime;

namespace Dynamic {

class Var;

}

namespace Data {


class Time;


class Data_API Date
	/// Date class wraps a DateTime and exposes date related interface.
	/// The purpose of this class is binding/extraction support for date fields.
{
public:
	Date();
		/// Creates the Date

	Date(int year, int month, int day);
		/// Creates the Date

	Date(const DateTime& dt);
		/// Creates the Date from DateTime

	~Date();
		/// Destroys the Date.

	int year() const;
		/// Returns the year.

	int month() const;
		/// Returns the month.

	int day() const;
		/// Returns the day.

	void assign(int year, int month, int day);
		/// Assigns date.

	Date& operator = (const Date& d);
		/// Assignment operator for Date.

	Date& operator = (const DateTime& dt);
		/// Assignment operator for DateTime.

	Date& operator = (const Poco::Dynamic::Var& var);
		/// Assignment operator for Var.

	bool operator == (const Date& date) const;
		/// Equality operator.

	bool operator != (const Date& date) const;
		/// Inequality operator.

	bool operator < (const Date& date) const;
		/// Less then operator.

	bool operator > (const Date& date) const;
		/// Greater then operator.

private:
	int _year;
	int _month;
	int _day;
};


//
// inlines
//
inline int Date::year() const
{
	return _year;
}


inline int Date::month() const
{
	return _month;
}


inline int Date::day() const
{
	return _day;
}


inline Date& Date::operator = (const Date& d)
{
	assign(d.year(), d.month(), d.day());
	return *this;
}


inline Date& Date::operator = (const DateTime& dt)
{
	assign(dt.year(), dt.month(), dt.day());
	return *this;
}


inline bool Date::operator == (const Date& date) const
{
	return _year == date.year() &&
		_month == date.month() &&
		_day == date.day();
}


inline bool Date::operator != (const Date& date) const
{
	return !(*this == date);
}


inline bool Date::operator > (const Date& date) const
{
	return !(*this == date) && !(*this < date);
}


} } // namespace Poco::Data


//
// VarHolderImpl<Date>
//


namespace Poco {
namespace Dynamic {


template <>
class VarHolderImpl<Poco::Data::Date>: public VarHolder
{
public:
	VarHolderImpl(const Poco::Data::Date& val): _val(val)
	{
	}

	~VarHolderImpl()
	{
	}
	
	const std::type_info& type() const
	{
		return typeid(Poco::Data::Date);
	}

	void convert(Poco::Timestamp& val) const
	{
		DateTime dt;
		dt.assign(_val.year(), _val.month(), _val.day());
		val = dt.timestamp();
	}

	void convert(Poco::DateTime& val) const
	{
		val.assign(_val.year(), _val.month(), _val.day());
	}

	void convert(Poco::LocalDateTime& val) const
	{
		val.assign(_val.year(), _val.month(), _val.day());
	}

	void convert(std::string& val) const
	{
		DateTime dt(_val.year(), _val.month(), _val.day());
		val = DateTimeFormatter::format(dt, "%Y/%m/%d");
	}

	VarHolder* clone(Placeholder<VarHolder>* pVarHolder = 0) const
	{
		return cloneHolder(pVarHolder, _val);
	}
	
	const Poco::Data::Date& value() const
	{
		return _val;
	}

private:
	VarHolderImpl();
	Poco::Data::Date _val;
};


} } // namespace Poco::Dynamic


#endif // Data_Date_INCLUDED
