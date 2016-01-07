//
// DateTimeParserTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/DateTimeParserTest.cpp#5 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DateTimeParserTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DateTimeParser.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTime.h"
#include "Poco/Timestamp.h"
#include "Poco/Exception.h"


using Poco::DateTime;
using Poco::DateTimeFormat;
using Poco::DateTimeParser;
using Poco::Timestamp;
using Poco::SyntaxException;


DateTimeParserTest::DateTimeParserTest(const std::string& name): CppUnit::TestCase(name)
{
}


DateTimeParserTest::~DateTimeParserTest()
{
}


void DateTimeParserTest::testISO8601()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FORMAT, "2005-01-08T12:30:00Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FORMAT, "2005-01-08T12:30:00+01:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FORMAT, "2005-01-08T12:30:00-01:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == -3600);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FORMAT, "2005-01-08T12:30:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FORMAT, "2005-01-08", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (tzd == 0);
}


void DateTimeParserTest::testISO8601Frac()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.microsecond() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00+01:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.microsecond() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00-01:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.microsecond() == 0);
	assert (tzd == -3600);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.microsecond() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.microsecond() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00.1Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 100);
	assert (dt.microsecond() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00.123+01:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00.12345-01:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 450);
	assert (tzd == -3600);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2010-09-23T16:17:01.2817002+02:00", tzd);
	assert (dt.year() == 2010);
	assert (dt.month() == 9);
	assert (dt.day() == 23);
	assert (dt.hour() == 16);
	assert (dt.minute() == 17);
	assert (dt.second() == 1);
	assert (dt.millisecond() == 281);
	assert (dt.microsecond() == 700);
	assert (tzd == 7200);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.microsecond() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08T12:30:00.123456", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 456);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::ISO8601_FRAC_FORMAT, "2005-01-08", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (dt.millisecond() == 0);
	assert (dt.microsecond() == 0);
	assert (tzd == 0);
}


void DateTimeParserTest::testRFC822()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::RFC822_FORMAT, "Sat, 8 Jan 05 12:30:00 GMT", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::RFC822_FORMAT, "Sat, 8 Jan 05 12:30:00 +0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::RFC822_FORMAT, "Sat, 8 Jan 05 12:30:00 -0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == -3600);

	dt = DateTimeParser::parse(DateTimeFormat::RFC822_FORMAT, "Tue, 18 Jan 05 12:30:00 CET", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 18);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::RFC822_FORMAT, "Wed, 12 Sep 73 02:01:12 CEST", tzd);
	assert (dt.year() == 1973);
	assert (dt.month() == 9);
	assert (dt.day() == 12);
	assert (dt.hour() == 2);
	assert (dt.minute() == 1);
	assert (dt.second() == 12);
	assert (tzd == 7200);

	dt = DateTimeParser::parse(DateTimeFormat::RFC822_FORMAT, "12 Sep 73 02:01:12 CEST", tzd);
	assert (dt.year() == 1973);
	assert (dt.month() == 9);
	assert (dt.day() == 12);
	assert (dt.hour() == 2);
	assert (dt.minute() == 1);
	assert (dt.second() == 12);
	assert (tzd == 7200);
}


void DateTimeParserTest::testRFC1123()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::RFC1123_FORMAT, "Sat, 8 Jan 2005 12:30:00 GMT", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::RFC1123_FORMAT, "Sat, 8 Jan 2005 12:30:00 +0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::RFC1123_FORMAT, "Sat, 8 Jan 2005 12:30:00 -0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == -3600);

	dt = DateTimeParser::parse(DateTimeFormat::RFC1123_FORMAT, "Sun, 20 Jul 1969 16:17:30 EDT", tzd);
	assert (dt.year() == 1969);
	assert (dt.month() == 7);
	assert (dt.day() == 20);
	assert (dt.hour() == 16);
	assert (dt.minute() == 17);
	assert (dt.second() == 30);
	assert (tzd == -14400);

	dt = DateTimeParser::parse(DateTimeFormat::RFC1123_FORMAT, "Sun, 20 Jul 1969 16:17:30 GMT+01:00", tzd);
	assert (dt.year() == 1969);
	assert (dt.month() == 7);
	assert (dt.day() == 20);
	assert (dt.hour() == 16);
	assert (dt.minute() == 17);
	assert (dt.second() == 30);
	assert (tzd == 3600);
}


void DateTimeParserTest::testHTTP()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::HTTP_FORMAT, "Sat, 08 Jan 2005 12:30:00 GMT", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::HTTP_FORMAT, "Sat, 08 Jan 2005 12:30:00 +0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::HTTP_FORMAT, "Sat, 08 Jan 2005 12:30:00 -0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == -3600);
}


void DateTimeParserTest::testRFC850()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::RFC850_FORMAT, "Saturday, 8-Jan-05 12:30:00 GMT", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::RFC850_FORMAT, "Saturday, 8-Jan-05 12:30:00 +0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::RFC850_FORMAT, "Saturday, 8-Jan-05 12:30:00 -0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == -3600);
	
	dt = DateTimeParser::parse(DateTimeFormat::RFC850_FORMAT, "Wed, 12-Sep-73 02:01:12 CEST", tzd);
	assert (dt.year() == 1973);
	assert (dt.month() == 9);
	assert (dt.day() == 12);
	assert (dt.hour() == 2);
	assert (dt.minute() == 1);
	assert (dt.second() == 12);
	assert (tzd == 7200);
}


void DateTimeParserTest::testRFC1036()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::RFC1036_FORMAT, "Saturday, 8 Jan 05 12:30:00 GMT", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::RFC1036_FORMAT, "Saturday, 8 Jan 05 12:30:00 +0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse(DateTimeFormat::RFC1036_FORMAT, "Saturday, 8 Jan 05 12:30:00 -0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == -3600);
}


void DateTimeParserTest::testASCTIME()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::ASCTIME_FORMAT, "Sat Jan  8 12:30:00 2005", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);
}


void DateTimeParserTest::testSORTABLE()
{
	int tzd;
	DateTime dt = DateTimeParser::parse(DateTimeFormat::SORTABLE_FORMAT, "2005-01-08 12:30:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse(DateTimeFormat::SORTABLE_FORMAT, "2005-01-08", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (tzd == 0);
}


void DateTimeParserTest::testCustom()
{
	int tzd;
	DateTime dt = DateTimeParser::parse("%d-%b-%Y", "18-Jan-2005", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 18);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (tzd == 0);
	
	dt = DateTimeParser::parse("%m/%d/%y", "01/18/05", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 18);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (tzd == 0);
	
	dt = DateTimeParser::parse("%h:%M %a", "12:30 am", tzd);
	assert (dt.hour() == 0);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);

	dt = DateTimeParser::parse("%h:%M %a", "12:30 PM", tzd);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);

	assert (!DateTimeParser::tryParse("%h:%M %a", "", dt, tzd));
	assert (!DateTimeParser::tryParse("", "12:30 PM", dt, tzd));
	assert (!DateTimeParser::tryParse("", "", dt, tzd));

	try
	{
		DateTimeParser::parse("%h:%M %a", "", tzd);
		fail ("must fail");
	}
	catch (SyntaxException&)
	{
	}

	try
	{
		DateTimeParser::parse("", "12:30 PM", tzd);
		fail ("must fail");
	}
	catch (SyntaxException&)
	{
	}

	try
	{
		DateTimeParser::parse("", "", tzd);
		fail ("must fail");
	}
	catch (SyntaxException&)
	{
	}
}


void DateTimeParserTest::testGuess()
{
	int tzd;
	DateTime dt = DateTimeParser::parse("2005-01-08T12:30:00Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse("20050108T123000Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);
	
	dt = DateTimeParser::parse("2005-01-08T12:30:00+01:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse("2005-01-08T12:30:00.123456Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 456);

	dt = DateTimeParser::parse("2005-01-08T12:30:00,123456Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 456);

	dt = DateTimeParser::parse("20050108T123000,123456Z", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 456);

	dt = DateTimeParser::parse("20050108T123000.123+0200", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 7200);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 0);


	dt = DateTimeParser::parse("2005-01-08T12:30:00.123456-02:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == -7200);
	assert (dt.millisecond() == 123);
	assert (dt.microsecond() == 456);

	dt = DateTimeParser::parse("Sat, 8 Jan 05 12:30:00 +0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);

	dt = DateTimeParser::parse("Sat, 8 Jan 2005 12:30:00 +0100", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 3600);
	
	dt = DateTimeParser::parse("Sat Jan  8 12:30:00 2005", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse("2005-01-08 12:30:00", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 12);
	assert (dt.minute() == 30);
	assert (dt.second() == 0);
	assert (tzd == 0);

	dt = DateTimeParser::parse("2005-01-08", tzd);
	assert (dt.year() == 2005);
	assert (dt.month() == 1);
	assert (dt.day() == 8);
	assert (dt.hour() == 0);
	assert (dt.minute() == 0);
	assert (dt.second() == 0);
	assert (tzd == 0);
}


void DateTimeParserTest::testParseMonth()
{
	std::string str = "January";
	std::string::const_iterator it = str.begin();
	int month = DateTimeParser::parseMonth(it, str.end());
	assert (month == 1);
	str = "February";
	it = str.begin();
	month = DateTimeParser::parseMonth(it, str.end());
	assert (month == 2);
	str = "December";
	it = str.begin();
	month = DateTimeParser::parseMonth(it, str.end());
	assert (month == 12);
	str = "Jan";
	it = str.begin();
	month = DateTimeParser::parseMonth(it, str.end());
	assert (month == 1);
	str = "Feb";
	it = str.begin();
	month = DateTimeParser::parseMonth(it, str.end());
	assert (month == 2);
	str = "jan";
	it = str.begin();
	month = DateTimeParser::parseMonth(it, str.end());
	assert (month == 1);
	str = "feb";
	it = str.begin();
	month = DateTimeParser::parseMonth(it, str.end());
	assert (month == 2);

	try
	{
		str = "ja";
		it = str.begin();
		month = DateTimeParser::parseMonth(it, str.end());
		fail("Not a valid month name - must throw");
	}
	catch (SyntaxException&)
	{
	}
}


void DateTimeParserTest::testParseDayOfWeek()
{
	std::string str = "Sunday";
	std::string::const_iterator it = str.begin();
	int dow = DateTimeParser::parseDayOfWeek(it, str.end());
	assert (dow == 0);
	str = "Monday";
	it = str.begin();
	dow = DateTimeParser::parseDayOfWeek(it, str.end());
	assert (dow == 1);
	str = "Saturday";
	it = str.begin();
	dow = DateTimeParser::parseDayOfWeek(it, str.end());
	assert (dow == 6);
	str = "Sun";
	it = str.begin();
	dow = DateTimeParser::parseDayOfWeek(it, str.end());
	assert (dow == 0);
	str = "Mon";
	it = str.begin();
	dow = DateTimeParser::parseDayOfWeek(it, str.end());
	assert (dow == 1);
	str = "sun";
	it = str.begin();
	dow = DateTimeParser::parseDayOfWeek(it, str.end());
	assert (dow == 0);
	str = "mon";
	it = str.begin();
	dow = DateTimeParser::parseDayOfWeek(it, str.end());
	assert (dow == 1);

	try
	{
		str = "su";
		it = str.begin();
		dow = DateTimeParser::parseDayOfWeek(it, str.end());
		fail("Not a valid weekday name - must throw");
	}
	catch (SyntaxException&)
	{
	}
}


void DateTimeParserTest::setUp()
{
}


void DateTimeParserTest::tearDown()
{
}


CppUnit::Test* DateTimeParserTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DateTimeParserTest");

	CppUnit_addTest(pSuite, DateTimeParserTest, testISO8601);
	CppUnit_addTest(pSuite, DateTimeParserTest, testISO8601Frac);
	CppUnit_addTest(pSuite, DateTimeParserTest, testRFC822);
	CppUnit_addTest(pSuite, DateTimeParserTest, testRFC1123);
	CppUnit_addTest(pSuite, DateTimeParserTest, testHTTP);
	CppUnit_addTest(pSuite, DateTimeParserTest, testRFC850);
	CppUnit_addTest(pSuite, DateTimeParserTest, testRFC1036);
	CppUnit_addTest(pSuite, DateTimeParserTest, testASCTIME);
	CppUnit_addTest(pSuite, DateTimeParserTest, testSORTABLE);
	CppUnit_addTest(pSuite, DateTimeParserTest, testCustom);
	CppUnit_addTest(pSuite, DateTimeParserTest, testGuess);
	CppUnit_addTest(pSuite, DateTimeParserTest, testParseMonth);
	CppUnit_addTest(pSuite, DateTimeParserTest, testParseDayOfWeek);

	return pSuite;
}
