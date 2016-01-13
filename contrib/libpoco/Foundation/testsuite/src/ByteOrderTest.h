//
// ByteOrderTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ByteOrderTest.h#1 $
//
// Definition of the ByteOrderTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ByteOrderTest_INCLUDED
#define ByteOrderTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ByteOrderTest: public CppUnit::TestCase
{
public:
	ByteOrderTest(const std::string& name);
	~ByteOrderTest();

	void testByteOrderFlip();
	void testByteOrderBigEndian();
	void testByteOrderLittleEndian();
	void testByteOrderNetwork();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ByteOrderTest_INCLUDED
