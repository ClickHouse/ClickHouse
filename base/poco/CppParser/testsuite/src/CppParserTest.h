//
// CppParserTest.h
//
// Definition of the CppParserTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParserTest_INCLUDED
#define CppParserTest_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "CppUnit/TestCase.h"


class CppParserTest: public CppUnit::TestCase
{
public:
	CppParserTest(const std::string& name);
	~CppParserTest();

	void testParseDir();
	void testExtractName();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // CppParserTest_INCLUDED
