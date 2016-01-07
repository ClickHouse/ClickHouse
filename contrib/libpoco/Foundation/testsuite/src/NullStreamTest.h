//
// NullStreamTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/NullStreamTest.h#1 $
//
// Definition of the NullStreamTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NullStreamTest_INCLUDED
#define NullStreamTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class NullStreamTest: public CppUnit::TestCase
{
public:
	NullStreamTest(const std::string& name);
	~NullStreamTest();

	void testInput();
	void testOutput();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NullStreamTest_INCLUDED
