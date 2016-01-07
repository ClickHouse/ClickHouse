//
// DateTimeFormat.cpp
//
// $Id: //poco/1.4/Foundation/src/DateTimeFormat.cpp#2 $
//
// Library: Foundation
// Package: DateTime
// Module:  DateTimeFormat
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DateTimeFormat.h"


namespace Poco {


const std::string DateTimeFormat::ISO8601_FORMAT("%Y-%m-%dT%H:%M:%S%z");
const std::string DateTimeFormat::ISO8601_FRAC_FORMAT("%Y-%m-%dT%H:%M:%s%z");
const std::string DateTimeFormat::RFC822_FORMAT("%w, %e %b %y %H:%M:%S %Z");
const std::string DateTimeFormat::RFC1123_FORMAT("%w, %e %b %Y %H:%M:%S %Z");
const std::string DateTimeFormat::HTTP_FORMAT("%w, %d %b %Y %H:%M:%S %Z");
const std::string DateTimeFormat::RFC850_FORMAT("%W, %e-%b-%y %H:%M:%S %Z");
const std::string DateTimeFormat::RFC1036_FORMAT("%W, %e %b %y %H:%M:%S %Z");
const std::string DateTimeFormat::ASCTIME_FORMAT("%w %b %f %H:%M:%S %Y");
const std::string DateTimeFormat::SORTABLE_FORMAT("%Y-%m-%d %H:%M:%S");


const std::string DateTimeFormat::WEEKDAY_NAMES[] =
{
	"Sunday",
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday"
};


const std::string DateTimeFormat::MONTH_NAMES[] =
{
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December"
};


} // namespace Poco
