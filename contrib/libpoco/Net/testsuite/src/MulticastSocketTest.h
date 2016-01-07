//
// MulticastSocketTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/MulticastSocketTest.h#1 $
//
// Definition of the MulticastSocketTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MulticastSocketTest_INCLUDED
#define MulticastSocketTest_INCLUDED


#include "Poco/Net/Net.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "CppUnit/TestCase.h"


class MulticastSocketTest: public CppUnit::TestCase
{
public:
	MulticastSocketTest(const std::string& name);
	~MulticastSocketTest();

	void testMulticast();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // POCO_NET_HAS_INTERFACE


#endif // MulticastSocketTest_INCLUDED
