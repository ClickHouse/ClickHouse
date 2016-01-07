//
// IPAddressTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/IPAddressTest.h#1 $
//
// Definition of the IPAddressTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef IPAddressTest_INCLUDED
#define IPAddressTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class IPAddressTest: public CppUnit::TestCase
{
public:
	IPAddressTest(const std::string& name);
	~IPAddressTest();

	void testStringConv();
	void testStringConv6();
	void testParse();
	void testClassification();
	void testMCClassification();
	void testClassification6();
	void testMCClassification6();
	void testRelationals();
	void testRelationals6();
	void testWildcard();
	void testBroadcast();
	void testPrefixCons();
	void testPrefixLen();
	void testOperators();
	void testByteOrderMacros();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // IPAddressTest_INCLUDED
