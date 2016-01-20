//
// LocalDateTime.h
//
// $Id: //poco/1.4/Foundation/include/Poco/LocalDateTime.h#1 $
//
// Library: Foundation
// Package: DateTime
// Module:  LocalDateTime
//
// Definition of the LocalDateTime class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_LocalDateTime_INCLUDED
#define Foundation_LocalDateTime_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DateTime.h"


namespace Poco {


class Foundation_API LocalDateTime
	/// This class represents an instant in local time
	/// (as opposed to UTC), expressed in years, months, days, 
	/// hours, minutes, seconds and milliseconds based on the 
	/// Gregorian calendar.
	///
	/// In addition to the date and time, the class also 
	/// maintains a time zone differential, which denotes
	/// the difference in seconds from UTC to local time,
	/// i.e. UTC = local time - time zone differential.
	///
	/// Although LocalDateTime supports relational and arithmetic
	/// operators, all date/time comparisons and date/time arithmetics
	/// should be done in UTC, using the DateTime or Timestamp
	/// class for better performance. The relational operators
	/// normalize the dates/times involved to UTC before carrying out
	/// the comparison.
	///
	/// The time zone differential is based on the input date and
	/// time and current time zone. A number of constructors accept
	/// an explicit time zone differential parameter. These should
	/// not be used since daylight savings time processing is impossible
	/// since the time zone is unknown. Each of the constructors
	/// accepting a tzd parameter have been marked as deprecated and
	/// may be removed in a future revision.
{
public:
	LocalDateTime();
		/// Creates a LocalDateTime with the current date/time 
		/// for the current time zone.

	LocalDateTime(int year, int month, int day, int hour = 0, int minute = 0, int second = 0, int millisecond = 0, int microsecond = 0);
		/// Creates a LocalDateTime for the given Gregorian local date and time.
		///   * year is from 0 to 9999.
		///   * month is from 1 to 12.
		///   * day is from 1 to 31.
		///   * hour is from 0 to 23.
		///   * minute is from 0 to 59.
		///   * second is from 0 to 59.
		///   * millisecond is from 0 to 999.
		///   * microsecond is from 0 to 999.

	//@ deprecated
	LocalDateTime(int tzd, int year, int month, int day, int hour, int minute, int second, int millisecond, int microsecond);
		/// Creates a LocalDateTime for the given Gregorian date and time in the
		/// time zone denoted by the time zone differential in tzd.
		///   * tzd is in seconds.
		///   * year is from 0 to 9999.
		///   * month is from 1 to 12.
		///   * day is from 1 to 31.
		///   * hour is from 0 to 23.
		///   * minute is from 0 to 59.
		///   * second is from 0 to 59.
		///   * millisecond is from 0 to 999.
		///   * microsecond is from 0 to 999.

	LocalDateTime(const DateTime& dateTime);
		/// Creates a LocalDateTime from the UTC time given in dateTime,
		/// using the time zone differential of the current time zone.

	//@ deprecated
	LocalDateTime(int tzd, const DateTime& dateTime);
		/// Creates a LocalDateTime from the UTC time given in dateTime,
		/// using the given time zone differential. Adjusts dateTime
		/// for the given time zone differential.

	//@ deprecated
	LocalDateTime(int tzd, const DateTime& dateTime, bool adjust);
		/// Creates a LocalDateTime from the UTC time given in dateTime,
		/// using the given time zone differential. If adjust is true, 
		/// adjusts dateTime for the given time zone differential.

	LocalDateTime(double julianDay);
		/// Creates a LocalDateTime for the given Julian day in the local time zone.

	//@ deprecated
	LocalDateTime(int tzd, double julianDay);
		/// Creates a LocalDateTime for the given Julian day in the time zone
		/// denoted by the time zone differential in tzd.

	LocalDateTime(const LocalDateTime& dateTime);
		/// Copy constructor. Creates the LocalDateTime from another one.
		
	~LocalDateTime();
		/// Destroys the LocalDateTime.
		
	LocalDateTime& operator = (const LocalDateTime& dateTime);
		/// Assigns another LocalDateTime.
	
	LocalDateTime& operator = (const Timestamp& timestamp);
		/// Assigns a timestamp

	LocalDateTime& operator = (double julianDay);
		/// Assigns a Julian day in the local time zone.
	
	LocalDateTime& assign(int year, int month, int day, int hour = 0, int minute = 0, int second = 0, int millisecond = 0, int microseconds = 0);
		/// Assigns a Gregorian local date and time.
		///   * year is from 0 to 9999.
		///   * month is from 1 to 12.
		///   * day is from 1 to 31.
		///   * hour is from 0 to 23.
		///   * minute is from 0 to 59.
		///   * second is from 0 to 59.
		///   * millisecond is from 0 to 999.
		///   * microsecond is from 0 to 999.

	//@ deprecated
	LocalDateTime& assign(int tzd, int year, int month, int day, int hour, int minute, int second, int millisecond, int microseconds);
		/// Assigns a Gregorian local date and time in the time zone denoted by
		/// the time zone differential in tzd.
		///   * tzd is in seconds.
		///   * year is from 0 to 9999.
		///   * month is from 1 to 12.
		///   * day is from 1 to 31.
		///   * hour is from 0 to 23.
		///   * minute is from 0 to 59.
		///   * second is from 0 to 59.
		///   * millisecond is from 0 to 999.
		///   * microsecond is from 0 to 999.

	//@ deprecated
	LocalDateTime& assign(int tzd, double julianDay);
		/// Assigns a Julian day in the time zone denoted by the
		/// time zone differential in tzd.

	void swap(LocalDateTime& dateTime);
		/// Swaps the LocalDateTime with another one.

	int year() const;
		/// Returns the year.
		
	int month() const;
		/// Returns the month (1 to 12).
	
	int week(int firstDayOfWeek = DateTime::MONDAY) const;
		/// Returns the week number within the year.
		/// FirstDayOfWeek should be either SUNDAY (0) or MONDAY (1).
		/// The returned week number will be from 0 to 53. Week number 1 is the week 
		/// containing January 4. This is in accordance to ISO 8601.
		/// 
		/// The following example assumes that firstDayOfWeek is MONDAY. For 2005, which started
		/// on a Saturday, week 1 will be the week starting on Monday, January 3.
		/// January 1 and 2 will fall within week 0 (or the last week of the previous year).
		///
		/// For 2007, which starts on a Monday, week 1 will be the week startung on Monday, January 1.
		/// There will be no week 0 in 2007.
	
	int day() const;
		/// Returns the day witin the month (1 to 31).
		
	int dayOfWeek() const;
		/// Returns the weekday (0 to 6, where
		/// 0 = Sunday, 1 = Monday, ..., 6 = Saturday).
	
	int dayOfYear() const;
		/// Returns the number of the day in the year.
		/// January 1 is 1, February 1 is 32, etc.
	
	int hour() const;
		/// Returns the hour (0 to 23).
		
	int hourAMPM() const;
		/// Returns the hour (0 to 12).
	
	bool isAM() const;
		/// Returns true if hour < 12;

	bool isPM() const;
		/// Returns true if hour >= 12.
		
	int minute() const;
		/// Returns the minute (0 to 59).
		
	int second() const;
		/// Returns the second (0 to 59).
		
	int millisecond() const;
		/// Returns the millisecond (0 to 999)
	
	int microsecond() const;
		/// Returns the microsecond (0 to 999)
	
	double julianDay() const;
		/// Returns the julian day for the date.

	int tzd() const;
		/// Returns the time zone differential.
		
	DateTime utc() const;
		/// Returns the UTC equivalent for the local date and time.

	Timestamp timestamp() const;
		/// Returns the date and time expressed as a Timestamp.

	Timestamp::UtcTimeVal utcTime() const;
		/// Returns the UTC equivalent for the local date and time.

	bool operator == (const LocalDateTime& dateTime) const;	
	bool operator != (const LocalDateTime& dateTime) const;	
	bool operator <  (const LocalDateTime& dateTime) const;	
	bool operator <= (const LocalDateTime& dateTime) const;	
	bool operator >  (const LocalDateTime& dateTime) const;	
	bool operator >= (const LocalDateTime& dateTime) const;	

	LocalDateTime  operator +  (const Timespan& span) const;
	LocalDateTime  operator -  (const Timespan& span) const;
	Timespan       operator -  (const LocalDateTime& dateTime) const;
	LocalDateTime& operator += (const Timespan& span);
	LocalDateTime& operator -= (const Timespan& span);

protected:
	LocalDateTime(Timestamp::UtcTimeVal utcTime, Timestamp::TimeDiff diff, int tzd);
		
	void determineTzd(bool adjust = false);
		/// Recalculate the tzd based on the _dateTime member based
		/// on the current timezone using the Standard C runtime functions.
		/// If adjust is true, then adjustForTzd() is called after the
		/// differential is calculated.
		
	void adjustForTzd();
		/// Adjust the _dateTime member based on the _tzd member.
		
	std::time_t dstOffset(int& dstOffset) const;
		/// Determine the DST offset for the current date/time.
	
private:
	DateTime _dateTime;
	int      _tzd;
	
	friend class DateTimeFormatter;
	friend class DateTimeParser;
};


//
// inlines
//
inline int LocalDateTime::year() const
{
	return _dateTime.year();
}


inline int LocalDateTime::month() const
{
	return _dateTime.month();
}


inline int LocalDateTime::week(int firstDayOfWeek) const
{
	return _dateTime.week(firstDayOfWeek);
}


inline int LocalDateTime::day() const
{
	return _dateTime.day();
}

	
inline int LocalDateTime::dayOfWeek() const
{
	return _dateTime.dayOfWeek();
}


inline int LocalDateTime::dayOfYear() const
{
	return _dateTime.dayOfYear();
}


inline int LocalDateTime::hour() const
{
	return _dateTime.hour();
}

	
inline int LocalDateTime::hourAMPM() const
{
	return _dateTime.hourAMPM();
}


inline bool LocalDateTime::isAM() const
{
	return _dateTime.isAM();
}


inline bool LocalDateTime::isPM() const
{
	return _dateTime.isPM();
}

	
inline int LocalDateTime::minute() const
{
	return _dateTime.minute();
}

	
inline int LocalDateTime::second() const
{
	return _dateTime.second();
}

	
inline int LocalDateTime::millisecond() const
{
	return _dateTime.millisecond();
}


inline int LocalDateTime::microsecond() const
{
	return _dateTime.microsecond();
}


inline double LocalDateTime::julianDay() const
{
	return _dateTime.julianDay();
}


inline int LocalDateTime::tzd() const
{
	return _tzd;
}


inline Timestamp LocalDateTime::timestamp() const
{
	return Timestamp::fromUtcTime(_dateTime.utcTime());
}


inline Timestamp::UtcTimeVal LocalDateTime::utcTime() const
{
	return _dateTime.utcTime() - ((Timestamp::TimeDiff) _tzd)*10000000;
}


inline void LocalDateTime::adjustForTzd()
{
	_dateTime += Timespan(((Timestamp::TimeDiff) _tzd)*Timespan::SECONDS);
}


inline void swap(LocalDateTime& d1, LocalDateTime& d2)
{
	d1.swap(d2);
}


} // namespace Poco


#endif // Foundation_LocalDateTime_INCLUDED
