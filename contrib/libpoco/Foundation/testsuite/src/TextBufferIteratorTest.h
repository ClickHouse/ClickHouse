//
// TextBufferIteratorTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TextBufferIteratorTest.h#1 $
//
// Definition of the TextBufferIteratorTest class.
//
// Copyright (c) 2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TextBufferIteratorTest_INCLUDED
#define TextBufferIteratorTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TextBufferIteratorTest: public CppUnit::TestCase
{
public:
	TextBufferIteratorTest(const std::string& name);
	~TextBufferIteratorTest();

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


#endif // TextBufferIteratorTest_INCLUDED
