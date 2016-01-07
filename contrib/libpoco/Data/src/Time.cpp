//
// Time.cpp
//
// $Id: //poco/Main/Data/src/Time.cpp#5 $
//
// Library: Data
// Package: DataCore
// Module:  Time
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/Time.h"
#include "Poco/Data/DynamicDateTime.h"
#include "Poco/DateTime.h"
#include "Poco/Dynamic/Var.h"


using Poco::DateTime;
using Poco::Dynamic::Var;


namespace Poco {
namespace Data {


Time::Time()
{
	DateTime dt;
	assign(dt.hour(), dt.minute(), dt.second());
}


Time::Time(int hour, int minute, int second)
{
	assign(hour, minute, second);
}


Time::Time(const DateTime& dt)
{
	assign(dt.hour(), dt.minute(), dt.second());
}


Time::~Time()
{
}


void Time::assign(int hour, int minute, int second)
{
	if (hour < 0 || hour > 23) 
		throw InvalidArgumentException("Hour must be between 0 and 23.");

	if (minute < 0 || minute > 59) 
		throw InvalidArgumentException("Minute must be between 0 and 59.");

	if (second < 0 || second > 59) 
		throw InvalidArgumentException("Second must be between 0 and 59.");

	_hour = hour;
	_minute = minute;
	_second = second;
}


bool Time::operator < (const Time& time) const
{
	int hour = time.hour();

	if (_hour < hour) return true;
	else if (_hour > hour) return false;
	else // hours equal
	{
		int minute = time.minute();
		if (_minute < minute) return true;
		else 
		if (_minute > minute) return false;
		else // minutes equal
		if (_second < time.second()) return true;
	}

	return false;
}


Time& Time::operator = (const Var& var)
{
#ifndef __GNUC__
// g++ used to choke on this, newer versions seem to digest it fine
// TODO: determine the version able to handle it properly
	*this = var.extract<Time>();
#else
	*this = var.operator Time(); 
#endif
	return *this;
}


} } // namespace Poco::Data


#ifdef __GNUC__
// only needed for g++ (see comment in Time::operator = above)

namespace Poco {
namespace Dynamic {


using Poco::Data::Time;
using Poco::DateTime;


template <>
Var::operator Time () const
{
	VarHolder* pHolder = content();

	if (!pHolder)
		throw InvalidAccessException("Can not convert empty value.");

	if (typeid(Time) == pHolder->type())
		return extract<Time>();
	else
	{
		Poco::DateTime result;
		pHolder->convert(result);
		return Time(result);
	}
}


} } // namespace Poco::Dynamic


#endif // __GNUC__
