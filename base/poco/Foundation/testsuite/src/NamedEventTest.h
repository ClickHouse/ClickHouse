//
// NamedEventTest.h
//
// Definition of the NamedEventTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NamedEventTest_INCLUDED
#define NamedEventTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class NamedEventTest: public CppUnit::TestCase
{
public:
	NamedEventTest(const std::string& name);
	~NamedEventTest();

	void testNamedEvent();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NamedEventTest_INCLUDED
