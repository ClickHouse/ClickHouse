//
// DocumentTypeTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/DocumentTypeTest.h#1 $
//
// Definition of the DocumentTypeTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DocumentTypeTest_INCLUDED
#define DocumentTypeTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class DocumentTypeTest: public CppUnit::TestCase
{
public:
	DocumentTypeTest(const std::string& name);
	~DocumentTypeTest();

	void testDocumentType();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // DocumentTypeTest_INCLUDED
