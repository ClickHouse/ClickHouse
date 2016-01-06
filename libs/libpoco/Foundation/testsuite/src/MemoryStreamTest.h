//
// MemoryStreamTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/MemoryStreamTest.h#1 $
//
// Definition of the MemoryStreamTest class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MemoryStreamTest_INCLUDED
#define MemoryStreamTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class MemoryStreamTest: public CppUnit::TestCase
{
public:
	MemoryStreamTest(const std::string& name);
	~MemoryStreamTest();

	void testInput();
	void testOutput();
	void testTell();
	void testInputSeek();
	void testInputSeekVsStringStream();
	void testOutputSeek();
	void testOutputSeekVsStringStream();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // MemoryStreamTest_INCLUDED
