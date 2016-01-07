//
// OptionTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/OptionTest.h#1 $
//
// Definition of the OptionTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef OptionTest_INCLUDED
#define OptionTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"


class OptionTest: public CppUnit::TestCase
{
public:
	OptionTest(const std::string& name);
	~OptionTest();

	void testOption();
	void testMatches1();
	void testMatches2();
	void testProcess1();
	void testProcess2();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // OptionTest_INCLUDED
