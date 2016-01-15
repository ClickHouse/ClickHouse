//
// FIFOBufferStreamTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/FIFOBufferStreamTest.h#1 $
//
// Definition of the FIFOBufferStreamTest class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef FIFOBufferStreamTest_INCLUDED
#define FIFOBufferStreamTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class FIFOBufferStreamTest: public CppUnit::TestCase
{
public:
	FIFOBufferStreamTest(const std::string& name);
	~FIFOBufferStreamTest();

	void testInput();
	void testOutput();
	void testNotify();

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


#endif // FIFOBufferStreamTest_INCLUDED
