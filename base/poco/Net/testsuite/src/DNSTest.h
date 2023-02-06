//
// DNSTest.h
//
// Definition of the DNSTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DNSTest_INCLUDED
#define DNSTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class DNSTest: public CppUnit::TestCase
{
public:
	DNSTest(const std::string& name);
	~DNSTest();

	void testHostByName();
	void testHostByAddress();
	void testResolve();
	void testEncodeIDN();
	void testDecodeIDN();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DNSTest_INCLUDED
