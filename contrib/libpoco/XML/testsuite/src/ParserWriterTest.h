//
// ParserWriterTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/ParserWriterTest.h#1 $
//
// Definition of the ParserWriterTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ParserWriterTest_INCLUDED
#define ParserWriterTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class ParserWriterTest: public CppUnit::TestCase
{
public:
	ParserWriterTest(const std::string& name);
	~ParserWriterTest();

	void testParseWriteXHTML();
	void testParseWriteXHTML2();
	void testParseWriteWSDL();
	void testParseWriteSimple();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	static const std::string XHTML;
	static const std::string XHTML2;
};


#endif // ParserWriterTest_INCLUDED
