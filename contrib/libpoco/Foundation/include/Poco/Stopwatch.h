//
// Stopwatch.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Stopwatch.h#2 $
//
// Library: Foundation
// Package: DateTime
// Module:  Stopwatch
//
// Definition of the Stopwatch class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Stopwatch_INCLUDED
#define Foundation_Stopwatch_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Clock.h"


namespace Poco {


class Foundation_API Stopwatch
	/// A simple facility to measure time intervals
	/// with microsecond resolution.
{
public:
	Stopwatch();
	~Stopwatch();

	void start();
		/// Starts (or restarts) the stopwatch.
		
	void stop();
		/// Stops or pauses the stopwatch.
	
	void reset();
		/// Resets the stopwatch.
		
	void restart();
		/// Resets and starts the stopwatch.
		
	Clock::ClockDiff elapsed() const;
		/// Returns the elapsed time in microseconds
		/// since the stopwatch started.
		
	int elapsedSeconds() const;
		/// Returns the number of seconds elapsed
		/// since the stopwatch started.

	static Clock::ClockVal resolution();
		/// Returns the resolution of the stopwatch.

private:
	Stopwatch(const Stopwatch&);
	Stopwatch& operator = (const Stopwatch&);

	Clock            _start;
	Clock::ClockDiff _elapsed;
	bool             _running;
};


//
// inlines
//
inline void Stopwatch::start()
{
	if (!_running)
	{
		_start.update();
		_running = true;
	}
}


inline void Stopwatch::stop()
{
	if (_running)
	{
		Clock current;
		_elapsed += current - _start;
		_running = false;
	}
}


inline int Stopwatch::elapsedSeconds() const
{
	return int(elapsed()/resolution());
}


inline Clock::ClockVal Stopwatch::resolution()
{
	return Clock::resolution();
}


} // namespace Poco


#endif // Foundation_Stopwatch_INCLUDED
