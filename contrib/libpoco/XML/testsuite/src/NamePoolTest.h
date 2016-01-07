//
// NamePoolTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/NamePoolTest.h#1 $
//
// Definition of the NamePoolTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NamePoolTest_INCLUDED
#define NamePoolTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class NamePoolTest: public CppUnit::TestCase
{
public:
	NamePoolTest(const std::string& name);
	~NamePoolTest();

	void testNamePool();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NamePoolTest_INCLUDED
