//
// DocumentTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/DocumentTest.h#1 $
//
// Definition of the DocumentTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DocumentTest_INCLUDED
#define DocumentTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class DocumentTest: public CppUnit::TestCase
{
public:
	DocumentTest(const std::string& name);
	~DocumentTest();

	void testDocumentElement();
	void testImport();
	void testImportDeep();
	void testElementsByTagName();
	void testElementsByTagNameNS();
	void testElementById();
	void testElementByIdNS();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DocumentTest_INCLUDED
