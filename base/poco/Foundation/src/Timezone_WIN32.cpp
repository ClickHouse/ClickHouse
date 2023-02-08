//
// Timezone_WIN32.cpp
//
// Library: Foundation
// Package: DateTime
// Module:  Timezone
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Timezone.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/Exception.h"
#include "Poco/UnWindows.h"
#include <ctime>


namespace Poco {


int Timezone::utcOffset()
{
	TIME_ZONE_INFORMATION tzInfo;
	DWORD dstFlag = GetTimeZoneInformation(&tzInfo);
	return -tzInfo.Bias*60;
}

	
int Timezone::dst()
{
	TIME_ZONE_INFORMATION tzInfo;
	DWORD dstFlag = GetTimeZoneInformation(&tzInfo);
	return dstFlag == TIME_ZONE_ID_DAYLIGHT ? -tzInfo.DaylightBias*60 : 0;
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
	std::string result;
	TIME_ZONE_INFORMATION tzInfo;
	DWORD dstFlag = GetTimeZoneInformation(&tzInfo);
	WCHAR* ptr = dstFlag == TIME_ZONE_ID_DAYLIGHT ? tzInfo.DaylightName : tzInfo.StandardName;
#if defined(POCO_WIN32_UTF8)
	UnicodeConverter::toUTF8(ptr, result);
#else
	char buffer[256];
	DWORD rc = WideCharToMultiByte(CP_ACP, 0, ptr, -1, buffer, sizeof(buffer), NULL, NULL);
	if (rc) result = buffer;
#endif
	return result;
}

	
std::string Timezone::standardName()
{
	std::string result;
	TIME_ZONE_INFORMATION tzInfo;
	DWORD dstFlag = GetTimeZoneInformation(&tzInfo);
	WCHAR* ptr = tzInfo.StandardName;
#if defined(POCO_WIN32_UTF8)
	UnicodeConverter::toUTF8(ptr, result);
#else
	char buffer[256];
	DWORD rc = WideCharToMultiByte(CP_ACP, 0, ptr, -1, buffer, sizeof(buffer), NULL, NULL);
	if (rc) result = buffer;
#endif
	return result;
}

	
std::string Timezone::dstName()
{
	std::string result;
	TIME_ZONE_INFORMATION tzInfo;
	DWORD dstFlag = GetTimeZoneInformation(&tzInfo);
	WCHAR* ptr = tzInfo.DaylightName;
#if defined(POCO_WIN32_UTF8)
	UnicodeConverter::toUTF8(ptr, result);
#else
	char buffer[256];
	DWORD rc = WideCharToMultiByte(CP_ACP, 0, ptr, -1, buffer, sizeof(buffer), NULL, NULL);
	if (rc) result = buffer;
#endif
	return result;
}


} // namespace Poco
