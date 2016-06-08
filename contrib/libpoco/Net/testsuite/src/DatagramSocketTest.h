//
// DatagramSocketTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/DatagramSocketTest.h#1 $
//
// Definition of the DatagramSocketTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DatagramSocketTest_INCLUDED
#define DatagramSocketTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class DatagramSocketTest: public CppUnit::TestCase
{
public:
	DatagramSocketTest(const std::string& name);
	~DatagramSocketTest();

	void testEcho();
	void testSendToReceiveFrom();
	void testBroadcast();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DatagramSocketTest_INCLUDED
