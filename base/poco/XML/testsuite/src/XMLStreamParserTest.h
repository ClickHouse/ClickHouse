//
// XMLStreamParserTest.h
//
// Definition of the XMLStreamParserTest class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XMLStreamParserTest_INCLUDED
#define XMLStreamParserTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class XMLStreamParserTest: public CppUnit::TestCase
{
public:
	XMLStreamParserTest(const std::string& name);
	~XMLStreamParserTest();

	void testParser();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // XMLStreamParserTest_INCLUDED
