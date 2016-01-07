//
// ValidatorTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/ValidatorTest.h#1 $
//
// Definition of the ValidatorTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ValidatorTest_INCLUDED
#define ValidatorTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"


class ValidatorTest: public CppUnit::TestCase
{
public:
	ValidatorTest(const std::string& name);
	~ValidatorTest();

	void testRegExpValidator();
	void testIntValidator();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ValidatorTest_INCLUDED
