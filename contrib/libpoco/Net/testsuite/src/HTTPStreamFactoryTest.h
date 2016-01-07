//
// HTTPStreamFactoryTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPStreamFactoryTest.h#1 $
//
// Definition of the HTTPStreamFactoryTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPStreamFactoryTest_INCLUDED
#define HTTPStreamFactoryTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPStreamFactoryTest: public CppUnit::TestCase
{
public:
	HTTPStreamFactoryTest(const std::string& name);
	~HTTPStreamFactoryTest();

	void testNoRedirect();
	void testEmptyPath();
	void testRedirect();
	void testProxy();
	void testError();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // HTTPStreamFactoryTest_INCLUDED
