//
// CoreTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/CoreTest.h#1 $
//
// Definition of the CoreTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CoreTest_INCLUDED
#define CoreTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class CoreTest: public CppUnit::TestCase
{
public:
	CoreTest(const std::string& name);
	~CoreTest();

	void testPlatform();
	void testFixedLength();
	void testBugcheck();
	void testFPE();
	void testEnvironment();
	void testBuffer();
	void testFIFOBufferChar();
	void testFIFOBufferInt();
	void testFIFOBufferEOFAndError();
	void testAtomicCounter();
	void testNullable();
	void testAscii();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

protected:
	void onReadable(bool& b);
	void onWritable(bool& b);

private:
	int _readableToNot;
	int _notToReadable;
	int _writableToNot;
	int _notToWritable;
};


#endif // CoreTest_INCLUDED
