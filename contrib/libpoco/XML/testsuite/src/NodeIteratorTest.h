//
// NodeIteratorTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/NodeIteratorTest.h#1 $
//
// Definition of the NodeIteratorTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NodeIteratorTest_INCLUDED
#define NodeIteratorTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class NodeIteratorTest: public CppUnit::TestCase
{
public:
	NodeIteratorTest(const std::string& name);
	~NodeIteratorTest();

	void testShowAll();
	void testShowElements();
	void testFilter();
	void testShowNothing();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NodeIteratorTest_INCLUDED
