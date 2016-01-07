//
// NetworkInterfaceTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/NetworkInterfaceTest.h#1 $
//
// Definition of the NetworkInterfaceTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetworkInterfaceTest_INCLUDED
#define NetworkInterfaceTest_INCLUDED


#include "Poco/Net/Net.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "CppUnit/TestCase.h"


class NetworkInterfaceTest: public CppUnit::TestCase
{
public:
	NetworkInterfaceTest(const std::string& name);
	~NetworkInterfaceTest();

	void testMap();
	void testList();
	void testForName();
	void testForAddress();
	void testForIndex();
	void testMapIpOnly();
	void testMapUpOnly();
	void testListMapConformance();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // POCO_NET_HAS_INTERFACE


#endif // NetworkInterfaceTest_INCLUDED
