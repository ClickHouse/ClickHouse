//
// RawSocketTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/RawSocketTest.h#1 $
//
// Definition of the RawSocketTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef RawSocketTest_INCLUDED
#define RawSocketTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class RawSocketTest: public CppUnit::TestCase
{
public:
	RawSocketTest(const std::string& name);
	~RawSocketTest();

	void testEchoIPv4();
	void testSendToReceiveFromIPv4();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // RawSocketTest_INCLUDED
