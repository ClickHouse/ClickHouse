//
// TextEncodingTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TextEncodingTest.h#1 $
//
// Definition of the TextEncodingTest class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TextEncodingTest_INCLUDED
#define TextEncodingTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TextEncodingTest: public CppUnit::TestCase
{
public:
	TextEncodingTest(const std::string& name);
	~TextEncodingTest();

	void testTextEncoding();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TextEncodingTest_INCLUDED
