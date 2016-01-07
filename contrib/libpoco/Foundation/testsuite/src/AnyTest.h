//
// AnyTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/AnyTest.h#1 $
//
// Tests for Any types
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#ifndef AnyTest_INCLUDED
#define AnyTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class AnyTest: public CppUnit::TestCase
{
public:
	AnyTest(const std::string& name);
	~AnyTest();

	void testConvertingCtor();
	void testDefaultCtor();
	void testCopyCtor();
	void testCopyAssign();
	void testConvertingAssign();
	void testBadCast();
	void testSwap();
	void testEmptyCopy();
	void testCastToReference();

	void testInt();
	void testComplexType();
	void testVector();
	
	void setUp();
	void tearDown();
	static CppUnit::Test* suite();
};


#endif // AnyTest_INCLUDED
