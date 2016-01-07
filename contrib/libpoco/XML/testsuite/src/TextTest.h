//
// TextTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/TextTest.h#1 $
//
// Definition of the TextTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TextTest_INCLUDED
#define TextTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class TextTest: public CppUnit::TestCase
{
public:
	TextTest(const std::string& name);
	~TextTest();

	void testLength();
	void testSubstring();
	void testAppend();
	void testInsert();
	void testDelete();
	void testReplace();
	void testSplit();
	void testSplitCDATA();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TextTest_INCLUDED
