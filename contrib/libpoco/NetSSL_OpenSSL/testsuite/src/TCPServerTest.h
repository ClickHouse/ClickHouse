//
// TCPServerTest.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/TCPServerTest.h#1 $
//
// Definition of the TCPServerTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TCPServerTest_INCLUDED
#define TCPServerTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class TCPServerTest: public CppUnit::TestCase
{
public:
	TCPServerTest(const std::string& name);
	~TCPServerTest();

	void testOneConnection();
	void testTwoConnections();
	void testMultiConnections();
	void testReuseSocket();
	void testReuseSession();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TCPServerTest_INCLUDED
