//
// Clock.cpp
//
// $Id: //poco/1.4/Foundation/src/Clock.cpp#3 $
//
// Library: Foundation
// Package: DateTime
// Module:  Clock
//
// Copyright (c) 2013, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Clock.h"
#include "Poco/Exception.h"
#include "Poco/Timestamp.h"
#if defined(__MACH__)
#include <mach/mach.h>
#include <mach/clock.h>
#elif defined(POCO_OS_FAMILY_UNIX)
#include <time.h>
#include <unistd.h>
#elif defined(POCO_VXWORKS)
#include <timers.h>
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "Poco/UnWindows.h"
#endif
#include <algorithm>
#undef min
#undef max
#include <limits>


namespace Poco {


const Clock::ClockVal Clock::CLOCKVAL_MIN = std::numeric_limits<Clock::ClockVal>::min();
const Clock::ClockVal Clock::CLOCKVAL_MAX = std::numeric_limits<Clock::ClockVal>::max();


Clock::Clock()
{
	update();
}


Clock::Clock(ClockVal tv)
{
	_clock = tv;
}


Clock::Clock(const Clock& other)
{
	_clock = other._clock;
}


Clock::~Clock()
{
}


Clock& Clock::operator = (const Clock& other)
{
	_clock = other._clock;
	return *this;
}


Clock& Clock::operator = (ClockVal tv)
{
	_clock = tv;
	return *this;
}


void Clock::swap(Clock& timestamp)
{
	std::swap(_clock, timestamp._clock);
}


void Clock::update()
{
#if defined(POCO_OS_FAMILY_WINDOWS)

	LARGE_INTEGER perfCounter;
	LARGE_INTEGER perfFreq;
	if (QueryPerformanceCounter(&perfCounter) && QueryPerformanceFrequency(&perfFreq))
	{
		_clock = resolution()*(perfCounter.QuadPart/perfFreq.QuadPart);
		_clock += (perfCounter.QuadPart % perfFreq.QuadPart)*resolution()/perfFreq.QuadPart;
	}
	else throw Poco::SystemException("cannot get system clock");

#elif defined(__MACH__)

	clock_serv_t cs;
	mach_timespec_t ts;

	host_get_clock_service(mach_host_self(), SYSTEM_CLOCK, &cs);
	clock_get_time(cs, &ts);
	mach_port_deallocate(mach_task_self(), cs);
	
	_clock = ClockVal(ts.tv_sec)*resolution() + ts.tv_nsec/1000;

#elif defined(POCO_VXWORKS)

	struct timespec ts;
#if defined(CLOCK_MONOTONIC) // should be in VxWorks 6.x
	if (clock_gettime(CLOCK_MONOTONIC, &ts))
		throw SystemException("cannot get system clock");
#else
	if (clock_gettime(CLOCK_REALTIME, &ts))
		throw SystemException("cannot get system clock");
#endif
	_clock = ClockVal(ts.tv_sec)*resolution() + ts.tv_nsec/1000;

#elif (defined(_POSIX_TIMERS) && defined(_POSIX_MONOTONIC_CLOCK)) || defined(__QNX__)

	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts))
		throw SystemException("cannot get system clock");
	_clock = ClockVal(ts.tv_sec)*resolution() + ts.tv_nsec/1000;

#else

	Poco::Timestamp now;
	_clock = now.epochMicroseconds();
	
#endif
}


Clock::ClockDiff Clock::accuracy()
{
#if defined(POCO_OS_FAMILY_WINDOWS)

	LARGE_INTEGER perfFreq;
	if (QueryPerformanceFrequency(&perfFreq) && perfFreq.QuadPart > 0)
	{
		ClockVal acc = resolution()/perfFreq.QuadPart;
		return acc > 0 ? acc : 1;
	}
	else throw Poco::SystemException("cannot get system clock accuracy");

#elif defined(__MACH__)

	clock_serv_t cs;
	int nanosecs;
	mach_msg_type_number_t n = 1;

	host_get_clock_service(mach_host_self(), SYSTEM_CLOCK, &cs);
	clock_get_attributes(cs, CLOCK_GET_TIME_RES, (clock_attr_t)&nanosecs, &n);
	mach_port_deallocate(mach_task_self(), cs);
	
	ClockVal acc = nanosecs/1000;
	return acc > 0 ? acc : 1;

#elif defined(POCO_VXWORKS)

	struct timespec ts;
#if defined(CLOCK_MONOTONIC) // should be in VxWorks 6.x
	if (clock_getres(CLOCK_MONOTONIC, &ts))
		throw SystemException("cannot get system clock");
#else
	if (clock_getres(CLOCK_REALTIME, &ts))
		throw SystemException("cannot get system clock");
#endif
	ClockVal acc = ClockVal(ts.tv_sec)*resolution() + ts.tv_nsec/1000;
	return acc > 0 ? acc : 1;

#elif defined(_POSIX_TIMERS) && defined(_POSIX_MONOTONIC_CLOCK)

	struct timespec ts;
	if (clock_getres(CLOCK_MONOTONIC, &ts))
		throw SystemException("cannot get system clock");
	
	ClockVal acc = ClockVal(ts.tv_sec)*resolution() + ts.tv_nsec/1000;
	return acc > 0 ? acc : 1;

#else

	return 1000;
	
#endif
}

	
bool Clock::monotonic()
{
#if defined(POCO_OS_FAMILY_WINDOWS)

	return true;

#elif defined(__MACH__)

	return true;

#elif defined(POCO_VXWORKS)

#if defined(CLOCK_MONOTONIC) // should be in VxWorks 6.x
	return true;
#else
	return false;
#endif

#elif defined(_POSIX_TIMERS) && defined(_POSIX_MONOTONIC_CLOCK)

	return true;

#else

	return false;
	
#endif
}


} // namespace Poco
