//
// SocketStreamTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/SocketStreamTest.h#1 $
//
// Definition of the SocketStreamTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SocketStreamTest_INCLUDED
#define SocketStreamTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class SocketStreamTest: public CppUnit::TestCase
{
public:
	SocketStreamTest(const std::string& name);
	~SocketStreamTest();

	void testStreamEcho();
	void testLargeStreamEcho();
	void testEOF();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SocketStreamTest_INCLUDED
