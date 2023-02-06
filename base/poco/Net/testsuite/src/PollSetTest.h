//
// PollSetTest.h
//
// Definition of the PollSetTest class.
//
// Copyright (c) 2016, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PollSetTest_INCLUDED
#define PollSetTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class PollSetTest: public CppUnit::TestCase
{
public:
	PollSetTest(const std::string& name);
	~PollSetTest();

	void testPoll();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();
};


#endif // PollSetTest_INCLUDED
