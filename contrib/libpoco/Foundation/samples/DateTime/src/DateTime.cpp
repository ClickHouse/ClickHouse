//
// DateTime.cpp
//
// $Id: //poco/1.4/Foundation/samples/DateTime/src/DateTime.cpp#1 $
//
// This sample demonstrates the DateTime class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/LocalDateTime.h"
#include "Poco/DateTime.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeParser.h"
#include <iostream>


using Poco::LocalDateTime;
using Poco::DateTime;
using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::DateTimeParser;


int main(int argc, char** argv)
{
	LocalDateTime now;
	
	std::string str = DateTimeFormatter::format(now, DateTimeFormat::ISO8601_FORMAT);
	DateTime dt;
	int tzd;
	DateTimeParser::parse(DateTimeFormat::ISO8601_FORMAT, str, dt, tzd);
	dt.makeUTC(tzd);
	LocalDateTime ldt(tzd, dt);
	return 0;
}
