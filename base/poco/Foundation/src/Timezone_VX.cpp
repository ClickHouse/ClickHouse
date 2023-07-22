//
// Timezone_VXX.cpp
//
// Library: Foundation
// Package: DateTime
// Module:  Timezone
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Timezone.h"
#include "Poco/Exception.h"
#include "Poco/Environment.h"
#include <ctime>


namespace Poco {


int Timezone::utcOffset()
{
	std::time_t now = std::time(NULL);
	struct std::tm t;
	gmtime_r(&now, &t);
	std::time_t utc = std::mktime(&t);
	return now - utc;
}

	
int Timezone::dst()
{
	std::time_t now = std::time(NULL);
	struct std::tm t;
	if (localtime_r(&now, &t) != OK)
		throw Poco::SystemException("cannot get local time DST offset");
	return t.tm_isdst == 1 ? 3600 : 0;
}


bool Timezone::isDst(const Timestamp& timestamp)
{
	std::time_t time = timestamp.epochTime();
	struct std::tm* tms = std::localtime(&time);
	if (!tms) throw Poco::SystemException("cannot get local time DST flag");
	return tms->tm_isdst > 0;
}

	
std::string Timezone::name()
{
	// format of TIMEZONE environment variable:
	// name_of_zone:<(unused)>:time_in_minutes_from_UTC:daylight_start:daylight_end
	std::string tz = Environment::get("TIMEZONE", "UTC");
	std::string::size_type pos = tz.find(':');
	if (pos != std::string::npos)
		return tz.substr(0, pos);
	else
		return tz;
}

	
std::string Timezone::standardName()
{
	return name();
}

	
std::string Timezone::dstName()
{
	return name();
}


} // namespace Poco
