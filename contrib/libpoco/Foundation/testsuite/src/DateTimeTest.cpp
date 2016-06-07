//
// DateTimeTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/DateTimeTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DateTimeTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DateTime.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
#include "Poco/Exception.h"


GCC_DIAG_OFF(unused-variable)


using Poco::Timestamp;
using Poco::DateTime;
using Poco::Timespan;
using Poco::AssertionViolationException;


DateTimeTest::DateTimeTest(const std::string& name): CppUnit::TestCase(name)
{
}


DateTimeTest::~DateTimeTest()
{
}


void DateTimeTest::testTimestamp()
{
	Timestamp ts(0); // Unix epoch 1970-01-01 00:00:00 Thursday
	DateTime dt(ts);
	assert (dt.year() == 1970);
	assert (dt.month() == 1);
	assert (dt.day() == 1);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.dayOfWeek() == 4);
	assert (dt.julianDay() == 2440587.5);
	assert (dt.timestamp() == 0);

	ts = Timestamp::fromEpochTime(1000000000);
	dt = ts; // 2001-09-09 01:46:40 Sunday
	assert (dt.year() == 2001);
	assert (dt.month() == 9);
	assert (dt.day() == 9);
	assert (dt.hour() == 1);
	assert (dt.minute() == 46);
	assert (dt.second() == 40);
	assert (dt.millisecond() == 0);
	assert (dt.dayOfWeek() == 0);
	assert (dt.timestamp().epochTime() == 1000000000);
	assertEqualDelta (dt.julianDay(), 2452161.574074, 0.000001);
}


void DateTimeTest::testJulian()
{
	DateTime dt(2440587.5); // unix epoch as Julian day
	assert (dt.year() == 1970);
	assert (dt.month() == 1);
	assert (dt.day() == 1);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.dayOfWeek() == 4);
	assert (dt.julianDay() == 2440587.5);
	assert (dt.timestamp() == 0);
	
	dt = 2299160.5; // 1582-10-15 00:00:00 (first day of Gregorian reform, UTC base)
	assert (dt.year() == 1582);
	assert (dt.month() == 10);
	assert (dt.day() == 15);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.dayOfWeek() == 5);
	assert (dt.julianDay() == 2299160.5);

	dt = 0.0; // -4713-11-24 12:00:00 (Gregorian date of Julian day reference)
	assert (dt.year() == -4713);
	assert (dt.month() == 11);
	assert (dt.day() == 24);
	assert (dt.hour() == 12);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.dayOfWeek() == 1);
	assert (dt.julianDay() == 0);

	// Test that we can represent down to the microsecond.
	dt = DateTime(2010, 1, 31, 17, 30, 15, 800, 3);

	assert (dt.year() == 2010);
	assert (dt.month() == 1);
	assert (dt.day() == 31);
	assert (dt.hour() == 17);
	assert (dt.minute() == 30);
	assert (dt.second() == 15);
	assert (dt.millisecond() == 800);
	assert (dt.microsecond() == 3);
}


void DateTimeTest::testGregorian()
{
	DateTime dt(1970, 1, 1);
	assert (dt.year() == 1970);
	assert (dt.month() == 1);
	assert (dt.day() == 1);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.dayOfWeek() == 4);
	assert (dt.julianDay() == 2440587.5);
	assert (dt.timestamp() == 0);
	
	dt.assign(2001, 9, 9, 1, 46, 40);
	assert (dt.year() == 2001);
	assert (dt.month() == 9);
	assert (dt.day() == 9);
	assert (dt.hour() == 1);
	assert (dt.minute() == 46);
	assert (dt.second() == 40);
	assert (dt.millisecond() == 0);
	assert (dt.dayOfWeek() == 0);
	assert (dt.timestamp().epochTime() == 1000000000);
	assertEqualDelta (dt.julianDay(), 2452161.574074, 0.000001);
}


void DateTimeTest::testConversions()
{
	DateTime dt1(2005, 1, 28, 14, 24, 44, 234);
	Timestamp ts1 = dt1.timestamp();
	DateTime dt2(ts1);
	Timestamp ts2 = dt2.timestamp();
	DateTime dt3;
	dt3 = dt1;
	Timestamp ts3 = dt3.timestamp();
	DateTime dt4(dt2);
	Timestamp ts4 = dt4.timestamp();

	assert (ts1 == ts2);
	assert (ts2 == ts3);
	assert (ts3 == ts4);

	assert (dt2.year() == 2005);
	assert (dt2.month() == 1);
	assert (dt2.day() == 28);
	assert (dt2.hour() == 14);
	assert (dt2.minute() == 24);
	assert (dt2.second() == 44);
	assert (dt2.millisecond() == 234);
	assert (dt2.dayOfWeek() == 5);
}


void DateTimeTest::testStatics()
{
	assert (DateTime::isLeapYear(1984));
	assert (DateTime::isLeapYear(1988));
	assert (DateTime::isLeapYear(1992));
	assert (DateTime::isLeapYear(1996));
	assert (DateTime::isLeapYear(2000));
	assert (DateTime::isLeapYear(2400));
	assert (!DateTime::isLeapYear(1995));
	assert (!DateTime::isLeapYear(1998));
	assert (!DateTime::isLeapYear(2001));
	assert (!DateTime::isLeapYear(1800));
	assert (!DateTime::isLeapYear(1900));
	
	assert (DateTime::daysOfMonth(2000, 1) == 31);
	assert (DateTime::daysOfMonth(2000, 2) == 29);
	assert (DateTime::daysOfMonth(1999, 2) == 28);
}


void DateTimeTest::testCalcs()
{
	DateTime dt1(2005, 1, 1);
	assert (dt1.dayOfYear() == 1);
	assert (dt1.week(DateTime::MONDAY) == 0);
	dt1.assign(2005, 1, 3);
	assert (dt1.dayOfYear() == 3);
	assert (dt1.week(DateTime::MONDAY) == 1);
	dt1.assign(2005, 1, 9);
	assert (dt1.dayOfYear() == 9);
	assert (dt1.week(DateTime::MONDAY) == 1);
	dt1.assign(2005, 1, 10);
	assert (dt1.dayOfYear() == 10);
	assert (dt1.week(DateTime::MONDAY) == 2);
	dt1.assign(2005, 2, 1);
	assert (dt1.dayOfYear() == 32);
	assert (dt1.week(DateTime::MONDAY) == 5);
	dt1.assign(2005, 12, 31);
	assert (dt1.week(DateTime::MONDAY) == 52);
	dt1.assign(2007, 1, 1);
	assert (dt1.week(DateTime::MONDAY) == 1);
	dt1.assign(2007, 12, 31);
	assert (dt1.week(DateTime::MONDAY) == 53);
	
	// Jan 1 is Mon
	dt1.assign(2001, 1, 1);  
	assert (dt1.week() == 1);
	dt1.assign(2001, 1, 7);
	assert (dt1.week() == 1);
	dt1.assign(2001, 1, 8);
	assert (dt1.week() == 2);
	dt1.assign(2001, 1, 21);
	assert (dt1.week() == 3);
	dt1.assign(2001, 1, 22);
	assert (dt1.week() == 4);

	// Jan 1 is Tue
	dt1.assign(2002, 1, 1);
	assert (dt1.week() == 1);
	dt1.assign(2002, 1, 6);
	assert (dt1.week() == 1);
	dt1.assign(2002, 1, 7);
	assert (dt1.week() == 2);
	dt1.assign(2002, 1, 20);
	assert (dt1.week() == 3);
	dt1.assign(2002, 1, 21);
	assert (dt1.week() == 4);

	// Jan 1 is Wed
	dt1.assign(2003, 1, 1);
	assert (dt1.week() == 1);
	dt1.assign(2003, 1, 5);
	assert (dt1.week() == 1);
	dt1.assign(2003, 1, 6);
	assert (dt1.week() == 2);
	dt1.assign(2003, 1, 19);
	assert (dt1.week() == 3);
	dt1.assign(2003, 1, 20);
	assert (dt1.week() == 4);
	
	// Jan 1 is Thu
	dt1.assign(2004, 1, 1);
	assert (dt1.week() == 1);
	dt1.assign(2004, 1, 4);
	assert (dt1.week() == 1);
	dt1.assign(2004, 1, 5);
	assert (dt1.week() == 2);
	dt1.assign(2004, 1, 18);
	assert (dt1.week() == 3);
	dt1.assign(2004, 1, 19);
	assert (dt1.week() == 4);

	// Jan 1 is Fri
	dt1.assign(1999, 1, 1);
	assert (dt1.week() == 0);
	dt1.assign(1999, 1, 3);
	assert (dt1.week() == 0);
	dt1.assign(1999, 1, 4);
	assert (dt1.week() == 1);
	dt1.assign(1999, 1, 17);
	assert (dt1.week() == 2);
	dt1.assign(1999, 1, 18);
	assert (dt1.week() == 3);

	// Jan 1 is Sat
	dt1.assign(2000, 1, 1);
	assert (dt1.week() == 0);
	dt1.assign(2000, 1, 2);
	assert (dt1.week() == 0);
	dt1.assign(2000, 1, 3);
	assert (dt1.week() == 1);
	dt1.assign(2000, 1, 16);
	assert (dt1.week() == 2);
	dt1.assign(2000, 1, 17);
	assert (dt1.week() == 3);
	
	// Jan 1 is Sun
	dt1.assign(1995, 1, 1);
	assert (dt1.week() == 0);
	dt1.assign(1995, 1, 2);
	assert (dt1.week() == 1);
	dt1.assign(1995, 1, 3);
	assert (dt1.week() == 1);
	dt1.assign(1995, 1, 15);
	assert (dt1.week() == 2);
	dt1.assign(1995, 1, 16);
	assert (dt1.week() == 3);
}


void DateTimeTest::testAMPM()
{
	DateTime dt1(2005, 1, 1, 0, 15, 30);
	assert (dt1.isAM());
	assert (!dt1.isPM());
	assert (dt1.hourAMPM() == 12);
	
	dt1.assign(2005, 1, 1, 12, 15, 30);
	assert (!dt1.isAM());
	assert (dt1.isPM());
	assert (dt1.hourAMPM() == 12);

	dt1.assign(2005, 1, 1, 13, 15, 30);
	assert (!dt1.isAM());
	assert (dt1.isPM());
	assert (dt1.hourAMPM() == 1);
}


void DateTimeTest::testRelational()
{
	DateTime dt1(2005, 1, 1, 0, 15, 30);
	DateTime dt2(2005, 1, 2, 0, 15, 30);
	DateTime dt3(dt1);
	
	assert (dt1 < dt2);
	assert (dt1 <= dt2);
	assert (dt2 > dt1);
	assert (dt2 >= dt1);
	assert (dt1 != dt2);
	assert (!(dt1 == dt2));
	
	assert (dt1 == dt3);
	assert (!(dt1 != dt3));
	assert (dt1 >= dt3);
	assert (dt1 <= dt3);
	assert (!(dt1 > dt3));
	assert (!(dt1 < dt3));

	static const struct 
	{
		int year;
		int month;  
		int day;
	} values[] = 
	{
		{	1,  1,  1 }, 
		{   10,  4,  5 }, 
		{  100,  6,  7 },
		{ 1000,  8,  9 }, 
		{ 2000,  1, 31 }, 
		{ 2002,  7,  4 },
		{ 2002, 12, 31 }, 
		{ 2003,  1,  1 }, 
		{ 2003,  1,  2 },
		{ 2003,  8,  5 }, 
		{ 2003,  8,  6 }, 
		{ 2003,  8,  7 },
		{ 2004,  9,  3 }, 
		{ 2004,  9,  4 }, 
	};

	const int num_values = sizeof values / sizeof *values;
	for (int i = 0; i < num_values; ++i) 
	{
		DateTime v;
		const DateTime& V = v;
		v.assign(values[i].year, values[i].month, values[i].day);
		for (int j = 0; j < num_values; ++j) 
		{
			DateTime u;  
			const DateTime& U = u;
			u.assign(values[j].year, values[j].month, values[j].day);

			loop_2_assert(i, j, (j <  i) == (U <  V));
			loop_2_assert(i, j, (j <= i) == (U <= V));
			loop_2_assert(i, j, (j >= i) == (U >= V));
			loop_2_assert(i, j, (j >  i) == (U >  V));
		}
	}
}


void DateTimeTest::testArithmetics()
{
	DateTime dt1(2005, 1, 1, 0, 15, 30);
	DateTime dt2(2005, 1, 2, 0, 15, 30);
	
	Timespan s = dt2 - dt1;
	assert (s.days() == 1);
	
	DateTime dt3 = dt1 + s;
	assert (dt3 == dt2);
	
	dt3 -= s;
	assert (dt3 == dt1);
	dt1 += s;
	assert (dt1 == dt2);

	static const struct 
	{
		int lineNum;		// source line number
		int year1;			// operand/result date1 year
		int month1;			// operand/result date1 month
		unsigned int day1;	// operand/result date1 day
		int numDays;		// operand/result 'int' number of days
		int year2;			// operand/result date2 year
		int month2;			// operand/result date2 month
		unsigned int day2;	// operand/result date2 day
	} data[] = 
	{
		//          - - - -first- - - -           - - - second - - - 
		//line no.  year   month   day   numDays  year   month   day
		//-------   -----  -----  -----  -------  -----  -----  ----- 
		{ __LINE__,       1,     1,     1,      1,      1,     1,     2 },
		{ __LINE__,      10,     2,    28,      1,     10,     3,     1 },
		{ __LINE__,     100,     3,    31,      2,    100,     4,     2 },
		{ __LINE__,    1000,     4,    30,      4,   1000,     5,     4 },
		{ __LINE__,    1000,     6,     1,    -31,   1000,     5,     1 },
		{ __LINE__,    1001,     1,     1,   -365,   1000,     1,     1 },
		{ __LINE__,    1100,     5,    31,     30,   1100,     6,    30 },
		{ __LINE__,    1200,     6,    30,     32,   1200,     8,     1 },
		{ __LINE__,    1996,     2,    28,    367,   1997,     3,     1 },
		{ __LINE__,    1997,     2,    28,    366,   1998,     3,     1 },
		{ __LINE__,    1998,     2,    28,    365,   1999,     2,    28 },
		{ __LINE__,    1999,     2,    28,    364,   2000,     2,    27 },
		{ __LINE__,    1999,     2,    28,   1096,   2002,     2,    28 },
		{ __LINE__,    2002,     2,    28,  -1096,   1999,     2,    28 },
	};

	const int num_data = sizeof data / sizeof *data;
	for (int di = 0; di < num_data; ++di) 
	{
		const int line	 = data[di].lineNum;
		const int num_days = data[di].numDays;
		DateTime x = DateTime(data[di].year1, data[di].month1, data[di].day1);
		const DateTime& X = x;
		x += Timespan(num_days, 0, 0, 0, 0);
		loop_1_assert(line, data[di].year2 == X.year());
		loop_1_assert(line, data[di].month2 == X.month());
		loop_1_assert(line, data[di].day2 == X.day());
	}

	DateTime edgeTime(2014, 9, 16, 0, 0, 0, 0, 10);
	edgeTime -= Poco::Timespan(11);
	assert (edgeTime.year() == 2014);
	assert (edgeTime.month() == 9);
	assert (edgeTime.day() == 15);
	assert (edgeTime.hour() == 23);
	assert (edgeTime.minute() == 59);
	assert (edgeTime.second() == 59);
	assert (edgeTime.millisecond() == 999);
	assert (edgeTime.microsecond() == 999);

	edgeTime.assign(2014, 9, 15, 23, 59, 59, 999, 968);
	edgeTime += Poco::Timespan(11);
	assert (edgeTime.year() == 2014);
	assert (edgeTime.month() == 9);
	assert (edgeTime.day() == 15);
	assert (edgeTime.hour() == 23);
	assert (edgeTime.minute() == 59);
	assert (edgeTime.second() == 59);
	assert (edgeTime.millisecond() == 999);
	assert (edgeTime.microsecond() == 979);
}

void DateTimeTest::testIncrementDecrement()
{
	static const struct 
	{
		int lineNum;		// source line number
		int year1;			// (first) date year
		int month1;			// (first) date month
		unsigned int day1;	// (first) date day
		int year2;			// (second) date year
		int month2;			// (second) date month
		unsigned int day2;	// (second) date day
	} data[] = 
	{
		 //          - - - -first- - - -    - - - second - - - 
		 //line no.  year   month   day     year   month   day
		 //-------   -----  -----  -----    -----  -----  ----- 
		{ __LINE__,       1,     1,     1,       1,     1,     2 },
		{ __LINE__,      10,     2,    28,      10,     3,     1 },
		{ __LINE__,     100,     3,    31,     100,     4,     1 },
		{ __LINE__,    1000,     4,    30,    1000,     5,     1 },
		{ __LINE__,    1100,     5,    31,    1100,     6,     1 },
		{ __LINE__,    1200,     6,    30,    1200,     7,     1 },
		{ __LINE__,    1300,     7,    31,    1300,     8,     1 },
		{ __LINE__,    1400,     8,    31,    1400,     9,     1 },
		{ __LINE__,    1500,     9,    30,    1500,    10,     1 },
		{ __LINE__,    1600,    10,    31,    1600,    11,     1 },
		{ __LINE__,    1700,    11,    30,    1700,    12,     1 },
		{ __LINE__,    1800,    12,    31,    1801,     1,     1 },
		{ __LINE__,    1996,     2,    28,    1996,     2,    29 },
		{ __LINE__,    1997,     2,    28,    1997,     3,     1 },
		{ __LINE__,    1998,     2,    28,    1998,     3,     1 },
		{ __LINE__,    1999,     2,    28,    1999,     3,     1 },
		{ __LINE__,    2000,     2,    28,    2000,     2,    29 },
		{ __LINE__,    2001,     2,    28,    2001,     3,     1 },
		{ __LINE__,    2004,     2,    28,    2004,     2,    29 },
		{ __LINE__,    2100,     2,    28,    2100,     3,     1 },
		{ __LINE__,    2400,     2,    28,    2400,     2,    29 },
	};

	const int num_data = sizeof data / sizeof *data;
	int di;

	for (di = 0; di < num_data; ++di) 
	{
		const int line = data[di].lineNum;
		DateTime x = DateTime(data[di].year1, data[di].month1, 
							  data[di].day1);
		// Would do pre-increment of x here.
		const DateTime& X = x;
		x = x + Timespan(1,0,0,0,0);  
		DateTime y = x; const DateTime& Y = y;

		loop_1_assert(line, data[di].year2  == X.year());
		loop_1_assert(line, data[di].month2 == X.month());
		loop_1_assert(line, data[di].day2   == X.day());

		loop_1_assert(line, data[di].year2  == Y.year());
		loop_1_assert(line, data[di].month2 == Y.month());
		loop_1_assert(line, data[di].day2   == Y.day());
	}

	for (di = 0; di < num_data; ++di) 
	{
		const int line = data[di].lineNum;
		DateTime x = DateTime(data[di].year1, data[di].month1, data[di].day1);
		DateTime x1 = DateTime(data[di].year1, data[di].month1, data[di].day1);
		DateTime x2 = DateTime(data[di].year2, data[di].month2, data[di].day2);
		DateTime y = x; const DateTime& Y = y;

		// Would do post increment of x here.
		const DateTime& X = x;
		x = x + Timespan(1,0,0,0,0);  

		loop_1_assert(line, data[di].year2  == X.year());
		loop_1_assert(line, data[di].month2 == X.month());
		loop_1_assert(line, data[di].day2   == X.day());
		loop_1_assert(line, data[di].year1  == Y.year());
		loop_1_assert(line, data[di].month1 == Y.month());
		loop_1_assert(line, data[di].day1   == Y.day());
	}
			
	for (di = 0; di < num_data; ++di) 
	{
		const int line = data[di].lineNum;
		DateTime x = DateTime(data[di].year2, data[di].month2, data[di].day2);
		const DateTime& X = x;
		x = x - Timespan(1,0,0,0,0);  
		DateTime y = x; DateTime Y = y;

		loop_1_assert(line, data[di].year1  == X.year());
		loop_1_assert(line, data[di].month1 == X.month());
		loop_1_assert(line, data[di].day1   == X.day());

		loop_1_assert(line, data[di].year1  == Y.year());
		loop_1_assert(line, data[di].month1 == Y.month());
		loop_1_assert(line, data[di].day1   == Y.day());
	}

	for (di = 0; di < num_data; ++di) 
	{
		const int line = data[di].lineNum;
		DateTime x1 = DateTime(data[di].year1, data[di].month1, data[di].day1);
		DateTime x = DateTime(data[di].year2, data[di].month2, data[di].day2);
		DateTime y = x; DateTime Y = y;
		const DateTime& X = x;
		// would post-decrement x here.
		x = x - Timespan(1,0,0,0,0);  

		loop_1_assert(line, data[di].year1  == X.year());
		loop_1_assert(line, data[di].month1 == X.month());
		loop_1_assert(line, data[di].day1   == X.day());

		loop_1_assert(line, data[di].year2  == Y.year());
		loop_1_assert(line, data[di].month2 == Y.month());
		loop_1_assert(line, data[di].day2   == Y.day());
	}
}


void DateTimeTest::testSwap()
{
	DateTime dt1(2005, 1, 1, 0, 15, 30);
	DateTime dt2(2005, 1, 2, 0, 15, 30);
	DateTime dt3(2005, 1, 1, 0, 15, 30);
	DateTime dt4(2005, 1, 2, 0, 15, 30);
	
	dt1.swap(dt2);
	assert (dt2 == dt3);
	assert (dt1 == dt4);
}


void DateTimeTest::testUsage()
{
	DateTime dt1(1776, 7, 4);
	assert (dt1.year() == 1776);
	assert (dt1.month() == 7);
	assert (dt1.day() == 4);

	DateTime dt2(dt1);
	dt2 += Timespan(6, 0, 0, 0, 0);
	assert (dt2.year() == 1776);
	assert (dt2.month() == 7);
	assert (dt2.day() == 10);

	Timespan span = dt2 - dt1;
	assert (span.days() == 6);

	// TODO - When adding months and years we need to be
	// able to specify the end-end convention.
	// We cannot do this in POCO at the moment.
}


void DateTimeTest::testSetYearDay()
{
	static const struct 
	{
		int d_lineNum;			// source line number
		int d_year;				// year under test
		unsigned int d_day;		// day-of-year under test
		int d_expMonth;			// expected month
		unsigned int d_expDay;	// expected day
	} data[] = 
	{
		//line no.  year   dayOfYr     exp. month   exp. day
		//-------   -----  -------     ----------   --------
		{ __LINE__,          1,       1,          1,         1 },   
		{ __LINE__,          1,       2,          1,         2 },
		{ __LINE__,          1,     365,         12,        31 },
		{ __LINE__,       1996,       1,          1,         1 },   
		{ __LINE__,       1996,       2,          1,         2 },
		{ __LINE__,       1996,     365,         12,        30 },
		{ __LINE__,       1996,     366,         12,        31 }
	};

	const int num_data = sizeof data / sizeof *data;
	for (int di = 0; di < num_data; ++di) 
	{
		const int line	  = data[di].d_lineNum;
		const int year	  = data[di].d_year;
		const unsigned int day = data[di].d_day;

		const int exp_month = data[di].d_expMonth;
		const unsigned int exp_day   = data[di].d_expDay;
		const DateTime r(year, exp_month, exp_day);
		DateTime x;
		const DateTime& X = x;

#if 0
		// TODO - need to be able to assign a day number in the year
		// but POCO is not able to do this.

		x.assign(year, day);				

		// TODO - need to be able to assert with the loop counter
		// but cppUnit is not able to do this.

		assert (r   == x);
		assert (day == X.dayOfYear());
#endif
	}

	static const struct 
	{
		int d_lineNum;  // source line number
		int d_year;	 // year under test
		int d_day;	  // day-of-year under test
		int d_exp;	  // expected status
	} data2[] = 
	{
		//line no.  year   dayOfYr     expected value
		//-------   -----  -------     --------------
		{ __LINE__,          1,       1,          1 },   
		{ __LINE__,          1,      -1,          0 },
		{ __LINE__,          1,       0,          0 },
		{ __LINE__,          1,     365,          1 },
		{ __LINE__,          1,     366,          0 },
		{ __LINE__,          1,     367,          0 },
		{ __LINE__,          0,       0,          0 },   
		{ __LINE__,         -1,      -1,          0 },   
		{ __LINE__,       1996,       1,          1 },   
		{ __LINE__,       1996,       2,          1 },
		{ __LINE__,       1996,      32,          1 },
		{ __LINE__,       1996,     365,          1 },
		{ __LINE__,       1996,     366,          1 },
		{ __LINE__,       1996,     367,          0 },
	};

	const int num_data2 = sizeof data2 / sizeof *data2;
	for (int di = 0; di < num_data2; ++di) 
	{
		const int line  = data2[di].d_lineNum;
		const int year  = data2[di].d_year;
		const int day   = data2[di].d_day;
		const int exp   = data2[di].d_exp;
		DateTime x;  
		const DateTime& X = x;
		if (1 == exp)
		{
			DateTime r;
			const DateTime& r2 = r;
#if 0
			r.set(year, day);
#endif
		}
	}
}


void DateTimeTest::testIsValid()
{
	static const struct 
	{
		int  d_lineNum;  // source line number
		int  d_year;	 // year under test
		int  d_month;	// month under test
		int  d_day;	  // day under test
		bool d_exp;	  // expected value
	} data[] = 
	{
		//line no.  year   month   day     expected value
		//-------   -----  -----  -----    --------------
		{ __LINE__,    0,     0,     0,      false },   
		{ __LINE__,    1,     1,     0,      false },   
		{ __LINE__,    1,     0,     1,      false },   
		{ __LINE__,    0,     1,     1,      true },   
		{ __LINE__,    1,     1,    -1,      false },   
		{ __LINE__,    1,    -1,     1,      false },   
		{ __LINE__, 2004,     1,    32,      false },   
		{ __LINE__, 2004,     2,    30,      false },   
		{ __LINE__, 2004,     3,    32,      false },   
		{ __LINE__, 2004,     4,    31,      false },   
		{ __LINE__, 2004,     5,    32,      false },   
		{ __LINE__, 2004,     6,    31,      false },   
		{ __LINE__, 2004,     7,    32,      false },   
		{ __LINE__, 2004,     8,    32,      false },   
		{ __LINE__, 2004,     9,    31,      false },   
		{ __LINE__, 2004,    10,    32,      false },   
		{ __LINE__, 2004,    11,    31,      false },   
		{ __LINE__, 2004,    12,    32,      false },   
		{ __LINE__,    0,    12,    31,      true },
		{ __LINE__,    0,     2,    29,      true },
		{ __LINE__,    1,     1,     1,      true }, 
		{ __LINE__, 2010,     1,     2,      true }, 
		{ __LINE__, 2011,     2,     5,      true }, 
		{ __LINE__, 2012,     3,    10,      true }, 
		{ __LINE__, 2013,     4,    17,      true }, 
		{ __LINE__, 2014,     5,    23,      true }, 
		{ __LINE__, 1600,     2,    29,      true },   
		{ __LINE__, 1700,     2,    29,      false },   
		{ __LINE__, 1800,     2,    29,      false },   
		{ __LINE__, 1900,     2,    29,      false },   
		{ __LINE__, 2000,     2,    29,      true },   
		{ __LINE__, 2100,     2,    29,      false },   
	};

	const int num_data = sizeof data / sizeof *data;
	for (int di = 0; di < num_data; ++di) 
	{
		const int  line  = data[di].d_lineNum;
		const int  year  = data[di].d_year;
		const int  month = data[di].d_month;
		const int  day   = data[di].d_day;
		const bool exp   = data[di].d_exp;

		bool isValid = DateTime::isValid(year, month, day);
		loop_1_assert(line, exp == isValid);
	}
}


void DateTimeTest::testDayOfWeek()
{
	typedef DateTime::DaysOfWeek DOW;

	static const struct 
	{
		int d_lineNum;	// source line number
		int d_year;		// year under test
		int d_month;	// month under test
		int d_day;		// day under test
		DOW d_expDay;	// number of days to be added
	} data[] = 
	{
		//Line no.    year   month   day    expDay
		//-------   -----  -----  -----   -------
		{ __LINE__,  1600,   1,       1,   DateTime::SATURDAY  },
		{ __LINE__,  1600,   1,       2,   DateTime::SUNDAY    },         
		{ __LINE__,  1600,   1,       3,   DateTime::MONDAY    },
		{ __LINE__,  1600,   1,       4,   DateTime::TUESDAY   },
		{ __LINE__,  1600,   1,       5,   DateTime::WEDNESDAY },     
		{ __LINE__,  1600,   1,       6,   DateTime::THURSDAY  },
		{ __LINE__,  1600,   1,       7,   DateTime::FRIDAY    },
		{ __LINE__,  1600,   1,       8,   DateTime::SATURDAY  },
		{ __LINE__,  1752,   8,      27,   DateTime::SUNDAY    },
		{ __LINE__,  1752,   8,      28,   DateTime::MONDAY    },
		{ __LINE__,  1752,   8,      29,   DateTime::TUESDAY   },
		{ __LINE__,  1752,   8,      30,   DateTime::WEDNESDAY },
		{ __LINE__,  1752,   8,      31,   DateTime::THURSDAY  },
		{ __LINE__,  1752,   9,       1,   DateTime::FRIDAY    },
		{ __LINE__,  1752,   9,       2,   DateTime::SATURDAY  },         
		{ __LINE__,  1752,   9,      14,   DateTime::THURSDAY  },
		{ __LINE__,  1752,   9,      15,   DateTime::FRIDAY    },
		{ __LINE__,  1752,   9,      16,   DateTime::SATURDAY  },     
		{ __LINE__,  1752,   9,      17,   DateTime::SUNDAY    },
		{ __LINE__,  1752,   9,      18,   DateTime::MONDAY    },
		{ __LINE__,  1752,   9,      19,   DateTime::TUESDAY   },
		{ __LINE__,  1999,  12,      28,   DateTime::TUESDAY   },
		{ __LINE__,  1999,  12,      29,   DateTime::WEDNESDAY },         
		{ __LINE__,  1999,  12,      30,   DateTime::THURSDAY  },
		{ __LINE__,  1999,  12,      31,   DateTime::FRIDAY    },
		{ __LINE__,  2000,   1,       1,   DateTime::SATURDAY  },     
		{ __LINE__,  2000,   1,       2,   DateTime::SUNDAY    },
		{ __LINE__,  2000,   1,       3,   DateTime::MONDAY    },
		{ __LINE__,  2000,   1,       4,   DateTime::TUESDAY   },
	};
			
	const int num_data = sizeof data / sizeof *data;
	for (int di = 0; di < num_data ; ++di) 
	{
		const int line = data[di].d_lineNum;
		DateTime x = DateTime(data[di].d_year, data[di].d_month, data[di].d_day);
		const DateTime& X = x;
		loop_1_assert(line, data[di].d_expDay == X.dayOfWeek());
	}
}


void DateTimeTest::testUTC()
{
	DateTime dt(2007, 3, 5, 12, 30, 00);

	assert (dt.hour() == 12);
	dt.makeUTC(3600);
	assert (dt.hour() == 11);
	dt.makeLocal(3600);
	assert (dt.hour() == 12);
}


void DateTimeTest::setUp()
{
}


void DateTimeTest::tearDown()
{
}


CppUnit::Test* DateTimeTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DateTimeTest");

	CppUnit_addTest(pSuite, DateTimeTest, testTimestamp);
	CppUnit_addTest(pSuite, DateTimeTest, testJulian);
	CppUnit_addTest(pSuite, DateTimeTest, testGregorian);
	CppUnit_addTest(pSuite, DateTimeTest, testConversions);
	CppUnit_addTest(pSuite, DateTimeTest, testStatics);
	CppUnit_addTest(pSuite, DateTimeTest, testCalcs);
	CppUnit_addTest(pSuite, DateTimeTest, testAMPM);
	CppUnit_addTest(pSuite, DateTimeTest, testRelational);
	CppUnit_addTest(pSuite, DateTimeTest, testArithmetics);
	CppUnit_addTest(pSuite, DateTimeTest, testSwap);

	CppUnit_addTest(pSuite, DateTimeTest, testUsage);
	CppUnit_addTest(pSuite, DateTimeTest, testSetYearDay);
	CppUnit_addTest(pSuite, DateTimeTest, testIsValid);
	CppUnit_addTest(pSuite, DateTimeTest, testDayOfWeek);
	CppUnit_addTest(pSuite, DateTimeTest, testIncrementDecrement);
	CppUnit_addTest(pSuite, DateTimeTest, testUTC);

	return pSuite;
}
