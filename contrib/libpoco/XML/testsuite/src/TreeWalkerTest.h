//
// TreeWalkerTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/TreeWalkerTest.h#1 $
//
// Definition of the TreeWalkerTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TreeWalkerTest_INCLUDED
#define TreeWalkerTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class TreeWalkerTest: public CppUnit::TestCase
{
public:
	TreeWalkerTest(const std::string& name);
	~TreeWalkerTest();

	void testShowAll();
	void testShowElements();
	void testFilter();
	void testShowNothing();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TreeWalkerTest_INCLUDED
