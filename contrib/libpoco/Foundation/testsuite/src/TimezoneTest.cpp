//
// TimezoneTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimezoneTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TimezoneTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Timezone.h"
#include <iostream>


using Poco::Timezone;


TimezoneTest::TimezoneTest(const std::string& name): CppUnit::TestCase(name)
{
}


TimezoneTest::~TimezoneTest()
{
}


void TimezoneTest::testTimezone()
{
	std::string name = Timezone::name();
	std::string stdName = Timezone::standardName();
	std::string dstName = Timezone::dstName();
	std::cout << "Timezone Names: " << name << ", " << stdName << ", " << dstName << std::endl;
	int utcOffset = Timezone::utcOffset();
	std::cout << "UTC Offset: " << utcOffset << std::endl;
	int dst = Timezone::dst();
	std::cout << "DST Offset: " << dst << std::endl;
}


void TimezoneTest::setUp()
{
}


void TimezoneTest::tearDown()
{
}


CppUnit::Test* TimezoneTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TimezoneTest");

	CppUnit_addTest(pSuite, TimezoneTest, testTimezone);

	return pSuite;
}
