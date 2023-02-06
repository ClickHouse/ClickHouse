//
// AttributesParserTest.h
//
// Definition of the AttributesParserTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef AttributesParserTest_INCLUDED
#define AttributesParserTest_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "CppUnit/TestCase.h"


class AttributesParserTest: public CppUnit::TestCase
{
public:
	AttributesParserTest(const std::string& name);
	~AttributesParserTest();

	void testParser1();
	void testParser2();
	void testParser3();
	void testParser4();
	void testParser5();
	void testParser6();
	void testParser7();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // AttributesParserTest_INCLUDED
