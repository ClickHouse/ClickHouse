//
// TextIteratorTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TextIteratorTest.h#1 $
//
// Definition of the TextIteratorTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TextIteratorTest_INCLUDED
#define TextIteratorTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TextIteratorTest: public CppUnit::TestCase
{
public:
	TextIteratorTest(const std::string& name);
	~TextIteratorTest();

	void testEmptyLatin1();
	void testOneLatin1();
	void testLatin1();
	void testEmptyUTF8();
	void testOneUTF8();
	void testUTF8();
	void testUTF8Supplementary();
	void testUTF16Supplementary();
	void testSwap();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TextIteratorTest_INCLUDED
