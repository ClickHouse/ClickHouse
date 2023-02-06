//
// SocketAddressTest.h
//
// Definition of the SocketAddressTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SocketAddressTest_INCLUDED
#define SocketAddressTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class SocketAddressTest: public CppUnit::TestCase
{
public:
	SocketAddressTest(const std::string& name);
	~SocketAddressTest();

	void testSocketAddress();
	void testSocketRelationals();
	void testSocketAddress6();
	void testSocketAddressUnixLocal();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // SocketAddressTest_INCLUDED
