//
// DateTimeFormatter.h
//
// Library: Foundation
// Package: DateTime
// Module:  DateTimeFormatter
//
// Definition of the DateTimeFormatter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DateTimeFormatter_INCLUDED
#define Foundation_DateTimeFormatter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DateTime.h"
#include "Poco/LocalDateTime.h"


namespace Poco {


class Timestamp;
class Timespan;


class Foundation_API DateTimeFormatter
	/// This class converts dates and times into strings, supporting a  
	/// variety of standard and custom formats.
	///
	/// There are two kind of static member functions:
	///    * format* functions return a std::string containing
	///      the formatted value.
	///    * append* functions append the formatted value to
	///      an existing string.
{
public:
	enum
	{
		UTC = 0xFFFF /// Special value for timeZoneDifferential denoting UTC. 
	};

	static std::string format(const Timestamp& timestamp, const std::string& fmt, int timeZoneDifferential = UTC);
		/// Formats the given timestamp according to the given format.
		/// The format string is used as a template to format the date and
		/// is copied character by character except for the following special characters,
		/// which are replaced by the corresponding value.
		///
		///   * %w - abbreviated weekday (Mon, Tue, ...)
		///   * %W - full weekday (Monday, Tuesday, ...)
		///   * %b - abbreviated month (Jan, Feb, ...)
		///   * %B - full month (January, February, ...)
		///   * %d - zero-padded day of month (01 .. 31)
		///   * %e - day of month (1 .. 31)
		///   * %f - space-padded day of month ( 1 .. 31)
		///   * %m - zero-padded month (01 .. 12)
		///   * %n - month (1 .. 12)
		///   * %o - space-padded month ( 1 .. 12)
		///   * %y - year without century (70)
		///   * %Y - year with century (1970)
		///   * %H - hour (00 .. 23)
		///   * %h - hour (00 .. 12)
		///   * %a - am/pm
		///   * %A - AM/PM
		///   * %M - minute (00 .. 59)
		///   * %S - second (00 .. 59)
		///   * %s - seconds and microseconds (equivalent to %S.%F)
		///   * %i - millisecond (000 .. 999)
		///   * %c - centisecond (0 .. 9)
		///   * %F - fractional seconds/microseconds (000000 - 999999)
		///   * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
		///   * %Z - time zone differential in RFC format (GMT or +NNNN)
		///   * %% - percent sign
		///
		/// Class DateTimeFormat defines format strings for various standard date/time formats.

	static std::string format(const DateTime& dateTime, const std::string& fmt, int timeZoneDifferential = UTC);
		/// Formats the given date and time according to the given format.
		/// See format(const Timestamp&, const std::string&, int) for more information.

	static std::string format(const LocalDateTime& dateTime, const std::string& fmt);
		/// Formats the given local date and time according to the given format.
		/// See format(const Timestamp&, const std::string&, int) for more information.

	static std::string format(const Timespan& timespan, const std::string& fmt = "%dd %H:%M:%S.%i");
		/// Formats the given timespan according to the given format.
		/// The format string is used as a template to format the date and
		/// is copied character by character except for the following special characters,
		/// which are replaced by the corresponding value.
		///
		///   * %d - days
		///   * %H - hours	 (00 .. 23)
		///   * %h - total hours (0 .. n)
		///   * %M - minutes (00 .. 59)
		///   * %m - total minutes (0 .. n)
		///   * %S - seconds (00 .. 59)
		///   * %s - total seconds (0 .. n)
		///   * %i - milliseconds (000 .. 999)
		///   * %c - centisecond (0 .. 9)
		///   * %F - fractional seconds/microseconds (000000 - 999999)
		///   * %% - percent sign

	static void append(std::string& str, const Timestamp& timestamp, const std::string& fmt, int timeZoneDifferential = UTC);
		/// Formats the given timestamp according to the given format and appends it to str.
		///
		/// See format() for documentation of the formatting string.

	static void append(std::string& str, const DateTime& dateTime, const std::string& fmt, int timeZoneDifferential = UTC);
		/// Formats the given date and time according to the given format and appends it to str.
		///
		/// See format() for documentation of the formatting string.

	static void append(std::string& str, const LocalDateTime& dateTime, const std::string& fmt);
		/// Formats the given local date and time according to the given format and appends it to str.
		///
		/// See format() for documentation of the formatting string.

	static void append(std::string& str, const Timespan& timespan, const std::string& fmt = "%dd %H:%M:%S.%i");
		/// Formats the given timespan according to the given format and appends it to str.
		///
		/// See format() for documentation of the formatting string.

	static std::string tzdISO(int timeZoneDifferential);
		/// Formats the given timezone differential in ISO format.
		/// If timeZoneDifferential is UTC, "Z" is returned,
		/// otherwise, +HH.MM (or -HH.MM) is returned.
		
	static std::string tzdRFC(int timeZoneDifferential);
		/// Formats the given timezone differential in RFC format.
		/// If timeZoneDifferential is UTC, "GMT" is returned,
		/// otherwise ++HHMM (or -HHMM) is returned.

	static void tzdISO(std::string& str, int timeZoneDifferential);
		/// Formats the given timezone differential in ISO format
		/// and appends it to the given string.
		/// If timeZoneDifferential is UTC, "Z" is returned,
		/// otherwise, +HH.MM (or -HH.MM) is returned.
		
	static void tzdRFC(std::string& str, int timeZoneDifferential);
		/// Formats the given timezone differential in RFC format
		/// and appends it to the given string.
		/// If timeZoneDifferential is UTC, "GMT" is returned,
		/// otherwise ++HHMM (or -HHMM) is returned.
};


//
// inlines
//
inline std::string DateTimeFormatter::format(const Timestamp& timestamp, const std::string& fmt, int timeZoneDifferential)
{
	DateTime dateTime(timestamp);
	return format(dateTime, fmt, timeZoneDifferential);
}


inline std::string DateTimeFormatter::format(const DateTime& dateTime, const std::string& fmt, int timeZoneDifferential)
{
	std::string result;
	result.reserve(64);
	append(result, dateTime, fmt, timeZoneDifferential);
	return result;
}


inline std::string DateTimeFormatter::format(const LocalDateTime& dateTime, const std::string& fmt)
{
	return format(dateTime._dateTime, fmt, dateTime._tzd);
}


inline std::string DateTimeFormatter::format(const Timespan& timespan, const std::string& fmt)
{
	std::string result;
	result.reserve(32);
	append(result, timespan, fmt);
	return result;
}


inline void DateTimeFormatter::append(std::string& str, const Timestamp& timestamp, const std::string& fmt, int timeZoneDifferential)
{
	DateTime dateTime(timestamp);
	append(str, dateTime, fmt, timeZoneDifferential);
}


inline std::string DateTimeFormatter::tzdISO(int timeZoneDifferential)
{
	std::string result;
	result.reserve(8);
	tzdISO(result, timeZoneDifferential);
	return result;
}


inline std::string DateTimeFormatter::tzdRFC(int timeZoneDifferential)
{
	std::string result;
	result.reserve(8);
	tzdRFC(result, timeZoneDifferential);
	return result;
}


} // namespace Poco


#endif // Foundation_DateTimeFormatter_INCLUDED
