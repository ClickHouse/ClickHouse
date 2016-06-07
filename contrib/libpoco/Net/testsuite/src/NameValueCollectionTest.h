//
// NameValueCollectionTest.h
//
// $Id: //poco/1.4/Net/testsuite/src/NameValueCollectionTest.h#1 $
//
// Definition of the NameValueCollectionTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NameValueCollectionTest_INCLUDED
#define NameValueCollectionTest_INCLUDED


#include "Poco/Net/Net.h"
#include "CppUnit/TestCase.h"


class NameValueCollectionTest: public CppUnit::TestCase
{
public:
	NameValueCollectionTest(const std::string& name);
	~NameValueCollectionTest();

	void testNameValueCollection();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NameValueCollectionTest_INCLUDED
