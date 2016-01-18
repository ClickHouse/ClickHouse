//
// NamedTuplesTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/NamedTuplesTest.h#1 $
//
// Definition of the NamedTuplesTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NamedTuplesTest_INCLUDED
#define NamedTuplesTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class NamedTuplesTest: public CppUnit::TestCase
{
public:
	NamedTuplesTest(const std::string& name);
	~NamedTuplesTest();

	void testNamedTuple1();
	void testNamedTuple2();
	void testNamedTuple3();
	void testNamedTuple4();
	void testNamedTuple5();
	void testNamedTuple6();
	void testNamedTuple7();
	void testNamedTuple8();
	void testNamedTuple9();
	void testNamedTuple10();
	void testNamedTuple11();
	void testNamedTuple12();
	void testNamedTuple13();
	void testNamedTuple14();
	void testNamedTuple15();
	void testNamedTuple16();
	void testNamedTuple17();
	void testNamedTuple18();
	void testNamedTuple19();
	void testNamedTuple20();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NamedTuplesTest_INCLUDED
