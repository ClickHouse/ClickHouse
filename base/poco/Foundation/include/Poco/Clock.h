//
// Clock.h
//
// Library: Foundation
// Package: DateTime
// Module:  Clock
//
// Definition of the Clock class.
//
// Copyright (c) 2013, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Clock_INCLUDED
#define Foundation_Clock_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Clock
	/// A Clock stores a monotonic* clock value
	/// with (theoretical) microseconds resolution.
	/// Clocks can be compared with each other
	/// and simple arithmetics are supported.
	///
	/// [*] Note that Clock values are only monotonic if
	/// the operating system provides a monotonic clock.
	/// The monotonic() function can be used to check whether
	/// the system's clock is monotonic.
	///
	/// Monotonic Clock is available on Windows, Linux, OS X 
	/// and on POSIX platforms supporting clock_gettime() with CLOCK_MONOTONIC.	  
	///
	/// Clock values are relative to a system-dependent epoch time
	/// (usually the system's startup time) and have no relation
	/// to the time of day.
{
public:
	typedef Int64 ClockVal;
		/// Monotonic clock value in microsecond resolution.

	typedef Int64 ClockDiff;
		/// Difference between two ClockVal values in microseconds.

	static const ClockVal CLOCKVAL_MIN; /// Minimum clock value.
	static const ClockVal CLOCKVAL_MAX; /// Maximum clock value.

	Clock();
		/// Creates a Clock with the current system clock value.
		
	Clock(ClockVal tv);
		/// Creates a Clock from the given clock value.
		
	Clock(const Clock& other);
		/// Copy constructor.
		
	~Clock();
		/// Destroys the Clock.
		
	Clock& operator = (const Clock& other);
	Clock& operator = (ClockVal tv);
	
	void swap(Clock& clock);
		/// Swaps the Clock with another one.
	
	void update();
		/// Updates the Clock with the current system clock.

	bool operator == (const Clock& ts) const;
	bool operator != (const Clock& ts) const;
	bool operator >  (const Clock& ts) const;
	bool operator >= (const Clock& ts) const;
	bool operator <  (const Clock& ts) const;
	bool operator <= (const Clock& ts) const;
	
	Clock  operator +  (ClockDiff d) const;
	Clock  operator -  (ClockDiff d) const;
	ClockDiff operator - (const Clock& ts) const;
	Clock& operator += (ClockDiff d);
	Clock& operator -= (ClockDiff d);
	
	ClockVal microseconds() const;
		/// Returns the clock value expressed in microseconds
		/// since the system-specific epoch time (usually system
		/// startup).

	ClockVal raw() const;
		/// Returns the clock value expressed in microseconds
		/// since the system-specific epoch time (usually system
		/// startup).
		///
		/// Same as microseconds().
	
	ClockDiff elapsed() const;
		/// Returns the time elapsed since the time denoted by
		/// the Clock instance. Equivalent to Clock() - *this.
	
	bool isElapsed(ClockDiff interval) const;
		/// Returns true iff the given interval has passed
		/// since the time denoted by the Clock instance.
	
	static ClockDiff resolution();
		/// Returns the resolution in units per second.
		/// Since the Clock clas has microsecond resolution,
		/// the returned value is always 1000000.
		
	static ClockDiff accuracy();
		/// Returns the system's clock accuracy in microseconds.
		
	static bool monotonic();
		/// Returns true iff the system's clock is monotonic.

private:
	ClockVal _clock;
};


//
// inlines
//
inline bool Clock::operator == (const Clock& ts) const
{
	return _clock == ts._clock;
}


inline bool Clock::operator != (const Clock& ts) const
{
	return _clock != ts._clock;
}


inline bool Clock::operator >  (const Clock& ts) const
{
	return _clock > ts._clock;
}


inline bool Clock::operator >= (const Clock& ts) const
{
	return _clock >= ts._clock;
}


inline bool Clock::operator <  (const Clock& ts) const
{
	return _clock < ts._clock;
}


inline bool Clock::operator <= (const Clock& ts) const
{
	return _clock <= ts._clock;
}


inline Clock Clock::operator + (Clock::ClockDiff d) const
{
	return Clock(_clock + d);
}


inline Clock Clock::operator - (Clock::ClockDiff d) const
{
	return Clock(_clock - d);
}


inline Clock::ClockDiff Clock::operator - (const Clock& ts) const
{
	return _clock - ts._clock;
}


inline Clock& Clock::operator += (Clock::ClockDiff d)
{
	_clock += d;
	return *this;
}


inline Clock& Clock::operator -= (Clock::ClockDiff d)
{
	_clock -= d;
	return *this;
}


inline Clock::ClockVal Clock::microseconds() const
{
	return _clock;
}


inline Clock::ClockDiff Clock::elapsed() const
{
	Clock now;
	return now - *this;
}


inline bool Clock::isElapsed(Clock::ClockDiff interval) const
{
	Clock now;
	Clock::ClockDiff diff = now - *this;
	return diff >= interval;
}


inline Clock::ClockDiff Clock::resolution()
{
	return 1000000;
}


inline void swap(Clock& s1, Clock& s2)
{
	s1.swap(s2);
}


inline Clock::ClockVal Clock::raw() const
{
	return _clock;
}


} // namespace Poco


#endif // Foundation_Clock_INCLUDED
