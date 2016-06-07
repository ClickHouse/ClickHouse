//
// NameTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/NameTest.h#1 $
//
// Definition of the NameTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NameTest_INCLUDED
#define NameTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class NameTest: public CppUnit::TestCase
{
public:
	NameTest(const std::string& name);
	~NameTest();

	void testSplit();
	void testLocalName();
	void testPrefix();
	void testName();
	void testCompare();
	void testSwap();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NameTest_INCLUDED
