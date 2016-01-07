//
// NodeTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/NodeTest.h#1 $
//
// Definition of the NodeTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NodeTest_INCLUDED
#define NodeTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class NodeTest: public CppUnit::TestCase
{
public:
	NodeTest(const std::string& name);
	~NodeTest();

	void testInsert();
	void testAppend();
	void testRemove();
	void testReplace();
	void testInsertFragment1();
	void testInsertFragment2();
	void testInsertFragment3();
	void testAppendFragment1();
	void testAppendFragment2();
	void testAppendFragment3();
	void testReplaceFragment1();
	void testReplaceFragment2();
	void testReplaceFragment3();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // NodeTest_INCLUDED
