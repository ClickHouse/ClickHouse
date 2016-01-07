//
// HTTPSStreamFactoryTest.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSStreamFactoryTest.h#1 $
//
// Definition of the HTTPSStreamFactoryTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPSStreamFactoryTest_INCLUDED
#define HTTPSStreamFactoryTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class HTTPSStreamFactoryTest: public CppUnit::TestCase
{
public:
	HTTPSStreamFactoryTest(const std::string& name);
	~HTTPSStreamFactoryTest();

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


#endif // HTTPSStreamFactoryTest_INCLUDED
