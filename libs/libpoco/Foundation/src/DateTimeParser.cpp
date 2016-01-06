//
// DateTimeParser.cpp
//
// $Id: //poco/1.4/Foundation/src/DateTimeParser.cpp#5 $
//
// Library: Foundation
// Package: DateTime
// Module:  DateTimeParser
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DateTimeParser.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTime.h"
#include "Poco/Exception.h"
#include "Poco/Ascii.h"


namespace Poco {


#define SKIP_JUNK() \
	while (it != end && !Ascii::isDigit(*it)) ++it


#define SKIP_DIGITS() \
	while (it != end && Ascii::isDigit(*it)) ++it


#define PARSE_NUMBER(var) \
	while (it != end && Ascii::isDigit(*it)) var = var*10 + ((*it++) - '0')


#define PARSE_NUMBER_N(var, n) \
	{ int i = 0; while (i++ < n && it != end && Ascii::isDigit(*it)) var = var*10 + ((*it++) - '0'); }


#define PARSE_FRACTIONAL_N(var, n) \
	{ int i = 0; while (i < n && it != end && Ascii::isDigit(*it)) { var = var*10 + ((*it++) - '0'); i++; } while (i++ < n) var *= 10; }


void DateTimeParser::parse(const std::string& fmt, const std::string& str, DateTime& dateTime, int& timeZoneDifferential)
{
	if (fmt.empty() || str.empty())
		throw SyntaxException("Empty string.");

	int year   = 0;
	int month  = 0;
	int day    = 0;
	int hour   = 0;
	int minute = 0;
	int second = 0;
	int millis = 0;
	int micros = 0;
	int tzd    = 0;

	std::string::const_iterator it   = str.begin();
	std::string::const_iterator end  = str.end();
	std::string::const_iterator itf  = fmt.begin();
	std::string::const_iterator endf = fmt.end();

	while (itf != endf && it != end)
	{
		if (*itf == '%')
		{
			if (++itf != endf)
			{
				switch (*itf)
				{
				case 'w':
				case 'W':
					while (it != end && Ascii::isSpace(*it)) ++it;
					while (it != end && Ascii::isAlpha(*it)) ++it;
					break;
				case 'b':
				case 'B':
					month = parseMonth(it, end);
					break;
				case 'd':
				case 'e':
				case 'f':
					SKIP_JUNK();
					PARSE_NUMBER_N(day, 2);
					break;
				case 'm':
				case 'n':
				case 'o':
					SKIP_JUNK();
					PARSE_NUMBER_N(month, 2);
					break;					 
				case 'y':
					SKIP_JUNK();
					PARSE_NUMBER_N(year, 2);
					if (year >= 69) 
						year += 1900;
					else
						year += 2000;
					break;
				case 'Y':
					SKIP_JUNK();
					PARSE_NUMBER_N(year, 4);
					break;
				case 'r':
					SKIP_JUNK();
					PARSE_NUMBER(year);
					if (year < 1000)
					{
						if (year >= 69) 
							year += 1900;
						else
							year += 2000;
					}
					break;
				case 'H':
				case 'h':
					SKIP_JUNK();
					PARSE_NUMBER_N(hour, 2);
					break;
				case 'a':
				case 'A':
					hour = parseAMPM(it, end, hour);
					break;
				case 'M':
					SKIP_JUNK();
					PARSE_NUMBER_N(minute, 2);
					break;
				case 'S':
					SKIP_JUNK();
					PARSE_NUMBER_N(second, 2);
					break;
				case 's':
					SKIP_JUNK();
					PARSE_NUMBER_N(second, 2);
					if (it != end && (*it == '.' || *it == ','))
					{
						++it;
						PARSE_FRACTIONAL_N(millis, 3);
						PARSE_FRACTIONAL_N(micros, 3);
						SKIP_DIGITS();
					}
					break;
				case 'i':
					SKIP_JUNK();
					PARSE_NUMBER_N(millis, 3);
					break;
				case 'c':
					SKIP_JUNK();
					PARSE_NUMBER_N(millis, 1);
					millis *= 100;
					break;
				case 'F':
					SKIP_JUNK();
					PARSE_FRACTIONAL_N(millis, 3);
					PARSE_FRACTIONAL_N(micros, 3);
					SKIP_DIGITS();
					break;
				case 'z':
				case 'Z':
					tzd = parseTZD(it, end);
					break;
				}
				++itf;
			}
		}
		else ++itf;
	}
	if (month == 0) month = 1;
	if (day == 0) day = 1;
	if (DateTime::isValid(year, month, day, hour, minute, second, millis, micros))
		dateTime.assign(year, month, day, hour, minute, second, millis, micros);
	else 
		throw SyntaxException("date/time component out of range");
	timeZoneDifferential = tzd;
}


DateTime DateTimeParser::parse(const std::string& fmt, const std::string& str, int& timeZoneDifferential)
{
	DateTime result;
	parse(fmt, str, result, timeZoneDifferential);
	return result;
}

	
bool DateTimeParser::tryParse(const std::string& fmt, const std::string& str, DateTime& dateTime, int& timeZoneDifferential)
{
	try
	{
		parse(fmt, str, dateTime, timeZoneDifferential);
	}
	catch (Exception&)
	{
		return false;
	}
	return true;
}


void DateTimeParser::parse(const std::string& str, DateTime& dateTime, int& timeZoneDifferential)
{
	if (!tryParse(str, dateTime, timeZoneDifferential))
		throw SyntaxException("Unsupported or invalid date/time format");
}

	
DateTime DateTimeParser::parse(const std::string& str, int& timeZoneDifferential)
{
	DateTime result;
	if (tryParse(str, result, timeZoneDifferential))
		return result;
	else
		throw SyntaxException("Unsupported or invalid date/time format");
}

	
bool DateTimeParser::tryParse(const std::string& str, DateTime& dateTime, int& timeZoneDifferential)
{
	if (str.length() < 4) return false;
	
	if (str[3] == ',')
		return tryParse("%w, %e %b %r %H:%M:%S %Z", str, dateTime, timeZoneDifferential);
	else if (str[3] == ' ')
		return tryParse(DateTimeFormat::ASCTIME_FORMAT, str, dateTime, timeZoneDifferential);
	else if (str.find(',') < 10)
		return tryParse("%W, %e %b %r %H:%M:%S %Z", str, dateTime, timeZoneDifferential);
	else if (Ascii::isDigit(str[0]))
	{
		if (str.find(' ') != std::string::npos || str.length() == 10)
			return tryParse(DateTimeFormat::SORTABLE_FORMAT, str, dateTime, timeZoneDifferential);
		else if (str.find('.') != std::string::npos || str.find(',') != std::string::npos)
			return tryParse(DateTimeFormat::ISO8601_FRAC_FORMAT, str, dateTime, timeZoneDifferential);
		else
			return tryParse(DateTimeFormat::ISO8601_FORMAT, str, dateTime, timeZoneDifferential);
	}
	else return false;
}


int DateTimeParser::parseTZD(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	struct Zone
	{
		const char* designator;
		int         timeZoneDifferential;
	};

	static Zone zones[] =
	{
		{"Z",           0},
		{"UT",          0},
		{"GMT",         0},
		{"BST",    1*3600},
		{"IST",    1*3600},
		{"WET",         0},
		{"WEST",   1*3600},
		{"CET",    1*3600},
		{"CEST",   2*3600},
		{"EET",    2*3600},
		{"EEST",   3*3600},
		{"MSK",    3*3600},
		{"MSD",    4*3600},
		{"NST",   -3*3600-1800},
		{"NDT",   -2*3600-1800},
		{"AST",   -4*3600},
		{"ADT",   -3*3600},
		{"EST",   -5*3600},
		{"EDT",   -4*3600},
		{"CST",   -6*3600},
		{"CDT",   -5*3600},
		{"MST",   -7*3600},
		{"MDT",   -6*3600},
		{"PST",   -8*3600},
		{"PDT",   -7*3600},
		{"AKST",  -9*3600},
		{"AKDT",  -8*3600},
		{"HST",  -10*3600},
		{"AEST",  10*3600},
		{"AEDT",  11*3600},
		{"ACST",   9*3600+1800},
		{"ACDT",  10*3600+1800},
		{"AWST",   8*3600},
		{"AWDT",   9*3600}
	};

	int tzd = 0;
	while (it != end && Ascii::isSpace(*it)) ++it;
	if (it != end)
	{
		if (Ascii::isAlpha(*it))
		{
			std::string designator;
			designator += *it++;
			if (it != end && Ascii::isAlpha(*it)) designator += *it++;
			if (it != end && Ascii::isAlpha(*it)) designator += *it++;
			if (it != end && Ascii::isAlpha(*it)) designator += *it++;
			for (unsigned i = 0; i < sizeof(zones)/sizeof(Zone); ++i)
			{
				if (designator == zones[i].designator)
				{
					tzd = zones[i].timeZoneDifferential;
					break;
				}
			}
		}
		if (it != end && (*it == '+' || *it == '-'))
		{
			int sign = *it == '+' ? 1 : -1;
			++it;
			int hours = 0;
			PARSE_NUMBER_N(hours, 2);
			if (it != end && *it == ':') ++it;
			int minutes = 0;
			PARSE_NUMBER_N(minutes, 2);
			tzd += sign*(hours*3600 + minutes*60);
		}
	}
	return tzd;
}


int DateTimeParser::parseMonth(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	std::string month;
	while (it != end && (Ascii::isSpace(*it) || Ascii::isPunct(*it))) ++it;
	bool isFirst = true;
	while (it != end && Ascii::isAlpha(*it)) 
	{
		char ch = (*it++);
		if (isFirst) { month += Ascii::toUpper(ch); isFirst = false; }
		else month += Ascii::toLower(ch);
	}
	if (month.length() < 3) throw SyntaxException("Month name must be at least three characters long", month);
	for (int i = 0; i < 12; ++i) 
	{
		if (DateTimeFormat::MONTH_NAMES[i].find(month) == 0)
			return i + 1;
	}
	throw SyntaxException("Not a valid month name", month);
}


int DateTimeParser::parseDayOfWeek(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	std::string dow;
	while (it != end && (Ascii::isSpace(*it) || Ascii::isPunct(*it))) ++it;
	bool isFirst = true;
	while (it != end && Ascii::isAlpha(*it)) 
	{
		char ch = (*it++);
		if (isFirst) { dow += Ascii::toUpper(ch); isFirst = false; }
		else dow += Ascii::toLower(ch);
	}
	if (dow.length() < 3) throw SyntaxException("Weekday name must be at least three characters long", dow);
	for (int i = 0; i < 7; ++i) 
	{
		if (DateTimeFormat::WEEKDAY_NAMES[i].find(dow) == 0)
			return i;
	}
	throw SyntaxException("Not a valid weekday name", dow);
}


int DateTimeParser::parseAMPM(std::string::const_iterator& it, const std::string::const_iterator& end, int hour)
{
	std::string ampm;
	while (it != end && (Ascii::isSpace(*it) || Ascii::isPunct(*it))) ++it;
	while (it != end && Ascii::isAlpha(*it)) 
	{
		char ch = (*it++);
		ampm += Ascii::toUpper(ch);
	}
	if (ampm == "AM")
	{
		if (hour == 12)
			return 0;
		else
			return hour;
	}
	else if (ampm == "PM")
	{
		if (hour < 12)
			return hour + 12;
		else
			return hour;
	}
	else throw SyntaxException("Not a valid AM/PM designator", ampm);
}


} // namespace Poco
