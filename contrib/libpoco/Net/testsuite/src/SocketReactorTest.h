//
// SocketReactorTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/SocketReactorTest.h#1 $
//
// Definition of the SocketReactorTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SocketReactorTest_INCLUDED
#define SocketReactorTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class SocketReactorTest: public CppUnit::TestCase
{
public:
	SocketReactorTest(const std::string& name);
	~SocketReactorTest();

	void testSocketReactor();
	void testSetSocketReactor();
	void testParallelSocketReactor();
	void testSocketConnectorFail();
	void testSocketConnectorTimeout();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SocketReactorTest_INCLUDED
