//
// Timestamp.h
//
// Library: Foundation
// Package: DateTime
// Module:  Timestamp
//
// Definition of the Timestamp class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Timestamp_INCLUDED
#define Foundation_Timestamp_INCLUDED


#include "Poco/Foundation.h"
#include <ctime>


namespace Poco {

class Timespan;


class Foundation_API Timestamp
	/// A Timestamp stores a monotonic* time value
	/// with (theoretical) microseconds resolution.
	/// Timestamps can be compared with each other
	/// and simple arithmetics are supported.
	///
	/// [*] Note that Timestamp values are only monotonic as
	/// long as the systems's clock is monotonic as well
	/// (and not, e.g. set back due to time synchronization
	/// or other reasons).
	///
	/// Timestamps are UTC (Coordinated Universal Time)
	/// based and thus independent of the timezone
	/// in effect on the system.
	///
	/// The internal reference time is the Unix epoch, 
	/// midnight, January 1, 1970.
{
public:
	typedef Int64 TimeVal; 
		/// Monotonic UTC time value in microsecond resolution,
		/// with base time midnight, January 1, 1970.
		
	typedef Int64 UtcTimeVal; 
		/// Monotonic UTC time value in 100 nanosecond resolution,
		/// with base time midnight, October 15, 1582.
		
	typedef Int64 TimeDiff;
		/// Difference between two TimeVal values in microseconds.

	static const TimeVal TIMEVAL_MIN; /// Minimum timestamp value.
	static const TimeVal TIMEVAL_MAX; /// Maximum timestamp value.

	Timestamp();
		/// Creates a timestamp with the current time.
		
	Timestamp(TimeVal tv);
		/// Creates a timestamp from the given time value
		/// (microseconds since midnight, January 1, 1970).
		
	Timestamp(const Timestamp& other);
		/// Copy constructor.
		
	~Timestamp();
		/// Destroys the timestamp
		
	Timestamp& operator = (const Timestamp& other);
	Timestamp& operator = (TimeVal tv);
	
	void swap(Timestamp& timestamp);
		/// Swaps the Timestamp with another one.
	
	void update();
		/// Updates the Timestamp with the current time.

	bool operator == (const Timestamp& ts) const;
	bool operator != (const Timestamp& ts) const;
	bool operator >  (const Timestamp& ts) const;
	bool operator >= (const Timestamp& ts) const;
	bool operator <  (const Timestamp& ts) const;
	bool operator <= (const Timestamp& ts) const;
	
	Timestamp  operator +  (TimeDiff d) const;
	Timestamp  operator +  (const Timespan& span) const;
	Timestamp  operator -  (TimeDiff d) const;
	Timestamp  operator -  (const Timespan& span) const;
	TimeDiff   operator -  (const Timestamp& ts) const;
	Timestamp& operator += (TimeDiff d);
	Timestamp& operator += (const Timespan& span);
	Timestamp& operator -= (TimeDiff d);
	Timestamp& operator -= (const Timespan& span);
	
	std::time_t epochTime() const;
		/// Returns the timestamp expressed in time_t.
		/// time_t base time is midnight, January 1, 1970.
		/// Resolution is one second.
		
	UtcTimeVal utcTime() const;
		/// Returns the timestamp expressed in UTC-based
		/// time. UTC base time is midnight, October 15, 1582.
		/// Resolution is 100 nanoseconds.
	
	TimeVal epochMicroseconds() const;
		/// Returns the timestamp expressed in microseconds
		/// since the Unix epoch, midnight, January 1, 1970.
	
	TimeDiff elapsed() const;
		/// Returns the time elapsed since the time denoted by
		/// the timestamp. Equivalent to Timestamp() - *this.
	
	bool isElapsed(TimeDiff interval) const;
		/// Returns true iff the given interval has passed
		/// since the time denoted by the timestamp.

	TimeVal raw() const;
		/// Returns the raw time value.
		///
		/// Same as epochMicroseconds().
	
	static Timestamp fromEpochTime(std::time_t t);
		/// Creates a timestamp from a std::time_t.
		
	static Timestamp fromUtcTime(UtcTimeVal val);
		/// Creates a timestamp from a UTC time value
		/// (100 nanosecond intervals since midnight,
		/// October 15, 1582).
		
	static TimeDiff resolution();
		/// Returns the resolution in units per second.
		/// Since the timestamp has microsecond resolution,
		/// the returned value is always 1000000.

#if defined(_WIN32)
	static Timestamp fromFileTimeNP(UInt32 fileTimeLow, UInt32 fileTimeHigh);
	void toFileTimeNP(UInt32& fileTimeLow, UInt32& fileTimeHigh) const;
#endif

private:
	TimeVal _ts;
};


//
// inlines
//
inline bool Timestamp::operator == (const Timestamp& ts) const
{
	return _ts == ts._ts;
}


inline bool Timestamp::operator != (const Timestamp& ts) const
{
	return _ts != ts._ts;
}


inline bool Timestamp::operator >  (const Timestamp& ts) const
{
	return _ts > ts._ts;
}


inline bool Timestamp::operator >= (const Timestamp& ts) const
{
	return _ts >= ts._ts;
}


inline bool Timestamp::operator <  (const Timestamp& ts) const
{
	return _ts < ts._ts;
}


inline bool Timestamp::operator <= (const Timestamp& ts) const
{
	return _ts <= ts._ts;
}


inline Timestamp Timestamp::operator + (Timestamp::TimeDiff d) const
{
	return Timestamp(_ts + d);
}


inline Timestamp Timestamp::operator - (Timestamp::TimeDiff d) const
{
	return Timestamp(_ts - d);
}


inline Timestamp::TimeDiff Timestamp::operator - (const Timestamp& ts) const
{
	return _ts - ts._ts;
}


inline Timestamp& Timestamp::operator += (Timestamp::TimeDiff d)
{
	_ts += d;
	return *this;
}


inline Timestamp& Timestamp::operator -= (Timestamp::TimeDiff d)
{
	_ts -= d;
	return *this;
}


inline std::time_t Timestamp::epochTime() const
{
	return std::time_t(_ts/resolution());
}


inline Timestamp::UtcTimeVal Timestamp::utcTime() const
{
	return _ts*10 + (TimeDiff(0x01b21dd2) << 32) + 0x13814000;
}


inline Timestamp::TimeVal Timestamp::epochMicroseconds() const
{
	return _ts;
}


inline Timestamp::TimeDiff Timestamp::elapsed() const
{
	Timestamp now;
	return now - *this;
}


inline bool Timestamp::isElapsed(Timestamp::TimeDiff interval) const
{
	Timestamp now;
	Timestamp::TimeDiff diff = now - *this;
	return diff >= interval;
}


inline Timestamp::TimeDiff Timestamp::resolution()
{
	return 1000000;
}


inline void swap(Timestamp& s1, Timestamp& s2)
{
	s1.swap(s2);
}


inline Timestamp::TimeVal Timestamp::raw() const
{
	return _ts;
}


} // namespace Poco


#endif // Foundation_Timestamp_INCLUDED
