//
// XMLWriterTest.h
//
// Definition of the XMLWriterTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XMLWriterTest_INCLUDED
#define XMLWriterTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class XMLWriterTest: public CppUnit::TestCase
{
public:
	XMLWriterTest(const std::string& name);
	~XMLWriterTest();

	void testTrivial();
	void testTrivialCanonical();
	void testTrivialDecl();
	void testTrivialDeclPretty();
	void testTrivialFragment();
	void testTrivialFragmentPretty();
	void testDTDPretty();
	void testDTD();
	void testDTDPublic();
	void testDTDNotation();
	void testDTDEntity();
	void testAttributes();
	void testAttributesPretty();
	void testData();
	void testEmptyData();
	void testDataPretty();
	void testEmptyDataPretty();
	void testComment();
	void testPI();
	void testCharacters();
	void testEmptyCharacters();
	void testCDATA();
	void testRawCharacters();
	void testAttributeCharacters();
	void testDefaultNamespace();
	void testQNamespaces();
	void testQNamespacesNested();
	void testNamespaces();
	void testNamespacesCanonical();
	void testAttributeNamespaces();
	void testAttributeNamespacesCanonical();
	void testNamespacesNested();
	void testNamespacesNestedCanonical();
	void testExplicitNamespaces();
	void testWellformed();
	void testWellformedNested();
	void testWellformedNamespace();
	void testEmpty();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // XMLWriterTest_INCLUDED
