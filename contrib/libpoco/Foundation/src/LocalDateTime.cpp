//
// LocalDateTime.cpp
//
// $Id: //poco/1.4/Foundation/src/LocalDateTime.cpp#2 $
//
// Library: Foundation
// Package: DateTime
// Module:  LocalDateTime
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/LocalDateTime.h"
#include "Poco/Timezone.h"
#include "Poco/Timespan.h"
#include "Poco/Exception.h"
#include <algorithm>
#include <ctime>
#if defined(_WIN32_WCE) && _WIN32_WCE < 0x800
#include "wce_time.h"
#endif


namespace Poco {


LocalDateTime::LocalDateTime()
{
	determineTzd(true);
}


LocalDateTime::LocalDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond, int microsecond):
	_dateTime(year, month, day, hour, minute, second, millisecond, microsecond)
{
	determineTzd();
}


LocalDateTime::LocalDateTime(int tzd, int year, int month, int day, int hour, int minute, int second, int millisecond, int microsecond):
	_dateTime(year, month, day, hour, minute, second, millisecond, microsecond),
	_tzd(tzd)
{
}


LocalDateTime::LocalDateTime(double julianDay):
	_dateTime(julianDay)
{
	determineTzd(true);
}


LocalDateTime::LocalDateTime(int tzd, double julianDay):
	_dateTime(julianDay),
	_tzd(tzd)
{
	adjustForTzd();
}


LocalDateTime::LocalDateTime(const DateTime& dateTime):
	_dateTime(dateTime)
{
	determineTzd(true);
}


LocalDateTime::LocalDateTime(int tzd, const DateTime& dateTime):
	_dateTime(dateTime),
	_tzd(tzd)
{
	adjustForTzd();
}


LocalDateTime::LocalDateTime(int tzd, const DateTime& dateTime, bool adjust):
	_dateTime(dateTime),
	_tzd(tzd)
{
	if (adjust)
		adjustForTzd();
}


LocalDateTime::LocalDateTime(const LocalDateTime& dateTime):
	_dateTime(dateTime._dateTime),
	_tzd(dateTime._tzd)
{
}


LocalDateTime::LocalDateTime(Timestamp::UtcTimeVal utcTime, Timestamp::TimeDiff diff, int tzd):
	_dateTime(utcTime, diff),
	_tzd(tzd)
{
	adjustForTzd();
}

	
LocalDateTime::~LocalDateTime()
{
}


LocalDateTime& LocalDateTime::operator = (const LocalDateTime& dateTime)
{
	if (&dateTime != this)
	{
		_dateTime = dateTime._dateTime;
		_tzd      = dateTime._tzd;
	}
	return *this;
}


LocalDateTime& LocalDateTime::operator = (const Timestamp& timestamp)
{
	if (timestamp != this->timestamp())
	{
		_dateTime = timestamp;
		determineTzd(true);
	}
	return *this;
}


LocalDateTime& LocalDateTime::operator = (double julianDay)
{
	_dateTime = julianDay;
	determineTzd(true);
	return *this;
}


LocalDateTime& LocalDateTime::assign(int year, int month, int day, int hour, int minute, int second, int millisecond, int microseconds)
{
	_dateTime.assign(year, month, day, hour, minute, second, millisecond, microseconds);
	determineTzd(false);
	return *this;
}


LocalDateTime& LocalDateTime::assign(int tzd, int year, int month, int day, int hour, int minute, int second, int millisecond, int microseconds)
{
	_dateTime.assign(year, month, day, hour, minute, second, millisecond, microseconds);
	_tzd = tzd;
	return *this;
}


LocalDateTime& LocalDateTime::assign(int tzd, double julianDay)
{
	_tzd      = tzd;
	_dateTime = julianDay;
	adjustForTzd();
	return *this;
}


void LocalDateTime::swap(LocalDateTime& dateTime)
{
	_dateTime.swap(dateTime._dateTime);
	std::swap(_tzd, dateTime._tzd);
}


DateTime LocalDateTime::utc() const
{
	return DateTime(_dateTime.utcTime(), -((Timestamp::TimeDiff) _tzd)*Timespan::SECONDS);
}


bool LocalDateTime::operator == (const LocalDateTime& dateTime) const
{
	return utcTime() == dateTime.utcTime();
}


bool LocalDateTime::operator != (const LocalDateTime& dateTime) const	
{
	return utcTime() != dateTime.utcTime();
}


bool LocalDateTime::operator <  (const LocalDateTime& dateTime) const	
{
	return utcTime() < dateTime.utcTime();
}


bool LocalDateTime::operator <= (const LocalDateTime& dateTime) const	
{
	return utcTime() <= dateTime.utcTime();
}


bool LocalDateTime::operator >  (const LocalDateTime& dateTime) const	
{
	return utcTime() > dateTime.utcTime();
}


bool LocalDateTime::operator >= (const LocalDateTime& dateTime) const	
{
	return utcTime() >= dateTime.utcTime();
}


LocalDateTime LocalDateTime::operator + (const Timespan& span) const
{
	// First calculate the adjusted UTC time, then calculate the
	// locally adjusted time by constructing a new LocalDateTime.
	DateTime tmp(utcTime(), span.totalMicroseconds());
	return LocalDateTime(tmp);
}


LocalDateTime LocalDateTime::operator - (const Timespan& span) const
{
	// First calculate the adjusted UTC time, then calculate the
	// locally adjusted time by constructing a new LocalDateTime.
	DateTime tmp(utcTime(), -span.totalMicroseconds());
	return LocalDateTime(tmp);
}


Timespan LocalDateTime::operator - (const LocalDateTime& dateTime) const
{
	return Timespan((utcTime() - dateTime.utcTime())/10);
}


LocalDateTime& LocalDateTime::operator += (const Timespan& span)
{
	// Use the same trick as in operator+. Create a UTC time, adjust
	// it for the span, and convert back to LocalDateTime. This will
	// recalculate the tzd correctly in the case where the addition
	// crosses a DST boundary.
	*this = DateTime(utcTime(), span.totalMicroseconds());
	return *this;
}


LocalDateTime& LocalDateTime::operator -= (const Timespan& span)
{
	// Use the same trick as in operator-. Create a UTC time, adjust
	// it for the span, and convert back to LocalDateTime. This will
	// recalculate the tzd correctly in the case where the subtraction
	// crosses a DST boundary.
	*this = DateTime(utcTime(), -span.totalMicroseconds());
	return *this;
}


void LocalDateTime::determineTzd(bool adjust)
{
	if (adjust)
	{
		std::time_t epochTime = _dateTime.timestamp().epochTime();
#if defined(_WIN32) || defined(POCO_NO_POSIX_TSF)
#if defined(_WIN32_WCE) && _WIN32_WCE < 0x800
		std::tm* broken = wceex_localtime(&epochTime);
#else
		std::tm* broken = std::localtime(&epochTime);
#endif
		if (!broken) throw Poco::SystemException("cannot get local time");
		_tzd = (Timezone::utcOffset() + ((broken->tm_isdst == 1) ? 3600 : 0));
#else
		std::tm broken;
#if defined(POCO_VXWORKS)
		if (localtime_r(&epochTime, &broken) != OK)
			throw Poco::SystemException("cannot get local time");
#else
		if (!localtime_r(&epochTime, &broken))
			throw Poco::SystemException("cannot get local time");
#endif
		_tzd = (Timezone::utcOffset() + ((broken.tm_isdst == 1) ? 3600 : 0));
#endif
		adjustForTzd();
	}
	else
	{
		int dst;
		dstOffset(dst);
		_tzd = (Timezone::utcOffset() + dst);
	}
}


std::time_t LocalDateTime::dstOffset(int& dstOffset) const
{
	std::time_t local;
	std::tm     broken;

	broken.tm_year  = (_dateTime.year() - 1900);
	broken.tm_mon   = (_dateTime.month() - 1);
	broken.tm_mday  = _dateTime.day();
	broken.tm_hour  = _dateTime.hour();
	broken.tm_min   = _dateTime.minute();
	broken.tm_sec   = _dateTime.second();
	broken.tm_isdst = -1;
#if defined(_WIN32_WCE) && _WIN32_WCE < 0x800
	local = wceex_mktime(&broken);
#else
	local = std::mktime(&broken);
#endif
	
	dstOffset = (broken.tm_isdst == 1) ? 3600 : 0;
	return local;
}


} // namespace Poco

