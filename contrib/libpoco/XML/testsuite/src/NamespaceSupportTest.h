//
// NamespaceSupportTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/NamespaceSupportTest.h#1 $
//
// Definition of the NamespaceSupportTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NamespaceSupportTest_INCLUDED
#define NamespaceSupportTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class NamespaceSupportTest: public CppUnit::TestCase
{
public:
	NamespaceSupportTest(const std::string& name);
	~NamespaceSupportTest();

	void testNamespaceSupport();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NamespaceSupportTest_INCLUDED
