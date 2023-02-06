//
// SyslogTest.h
//
// Definition of the SyslogTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SyslogTest_INCLUDED
#define SyslogTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class SyslogTest: public CppUnit::TestCase
{
public:
	SyslogTest(const std::string& name);
	~SyslogTest();

	void testListener();
	void testChannelOpenClose();
	void testOldBSD();
	void testStructuredData();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SyslogTest_INCLUDED
