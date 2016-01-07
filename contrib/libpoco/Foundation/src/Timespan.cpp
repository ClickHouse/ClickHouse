//
// Timespan.cpp
//
// $Id: //poco/1.4/Foundation/src/Timespan.cpp#1 $
//
// Library: Foundation
// Package: DateTime
// Module:  Timespan
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Timespan.h"
#include <algorithm>


namespace Poco {


const Timespan::TimeDiff Timespan::MILLISECONDS = 1000;
const Timespan::TimeDiff Timespan::SECONDS      = 1000*Timespan::MILLISECONDS;
const Timespan::TimeDiff Timespan::MINUTES      =   60*Timespan::SECONDS;
const Timespan::TimeDiff Timespan::HOURS        =   60*Timespan::MINUTES;
const Timespan::TimeDiff Timespan::DAYS         =   24*Timespan::HOURS;


Timespan::Timespan():
	_span(0)
{
}

	
Timespan::Timespan(TimeDiff microSeconds):
	_span(microSeconds)
{
}


Timespan::Timespan(long seconds, long microSeconds):
	_span(TimeDiff(seconds)*SECONDS + microSeconds)
{
}

	
Timespan::Timespan(int days, int hours, int minutes, int seconds, int microSeconds):
	_span(TimeDiff(microSeconds) + TimeDiff(seconds)*SECONDS + TimeDiff(minutes)*MINUTES + TimeDiff(hours)*HOURS + TimeDiff(days)*DAYS)
{
}


Timespan::Timespan(const Timespan& timespan):
	_span(timespan._span)
{
}


Timespan::~Timespan()
{
}


Timespan& Timespan::operator = (const Timespan& timespan)
{
	_span = timespan._span;
	return *this;
}


Timespan& Timespan::operator = (TimeDiff microSeconds)
{
	_span = microSeconds;
	return *this;
}


Timespan& Timespan::assign(int days, int hours, int minutes, int seconds, int microSeconds)
{
	_span = TimeDiff(microSeconds) + TimeDiff(seconds)*SECONDS + TimeDiff(minutes)*MINUTES + TimeDiff(hours)*HOURS + TimeDiff(days)*DAYS;
	return *this;
}


Timespan& Timespan::assign(long seconds, long microSeconds)
{
	_span = TimeDiff(seconds)*SECONDS + TimeDiff(microSeconds);
	return *this;
}


void Timespan::swap(Timespan& timespan)
{
	std::swap(_span, timespan._span);
}


Timespan Timespan::operator + (const Timespan& d) const
{
	return Timespan(_span + d._span);
}


Timespan Timespan::operator - (const Timespan& d) const
{
	return Timespan(_span - d._span);
}


Timespan& Timespan::operator += (const Timespan& d)
{
	_span += d._span;
	return *this;
}


Timespan& Timespan::operator -= (const Timespan& d)
{
	_span -= d._span;
	return *this;
}


Timespan Timespan::operator + (TimeDiff microSeconds) const
{
	return Timespan(_span + microSeconds);
}


Timespan Timespan::operator - (TimeDiff microSeconds) const
{
	return Timespan(_span - microSeconds);
}


Timespan& Timespan::operator += (TimeDiff microSeconds)
{
	_span += microSeconds;
	return *this;
}


Timespan& Timespan::operator -= (TimeDiff microSeconds)
{
	_span -= microSeconds;
	return *this;
}


} // namespace Poco
