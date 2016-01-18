//
// WebSocketTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/WebSocketTest.h#1 $
//
// Definition of the WebSocketTest class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef WebSocketTest_INCLUDED
#define WebSocketTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class WebSocketTest: public CppUnit::TestCase
{
public:
	WebSocketTest(const std::string& name);
	~WebSocketTest();

	void testWebSocket();
	void testWebSocketLarge();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // WebSocketTest_INCLUDED
