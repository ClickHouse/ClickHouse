//
// ElementTest.h
//
// $Id: //poco/1.4/XML/testsuite/src/ElementTest.h#1 $
//
// Definition of the ElementTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ElementTest_INCLUDED
#define ElementTest_INCLUDED


#include "Poco/XML/XML.h"
#include "CppUnit/TestCase.h"


class ElementTest: public CppUnit::TestCase
{
public:
	ElementTest(const std::string& name);
	~ElementTest();

	void testAttributes();
	void testAttributesNS();
	void testAttrMap();
	void testAttrMapNS();
	void testElementsByTagName();
	void testElementsByTagNameNS();
	void testInnerText();
	void testChildElement();
	void testChildElementNS();
	void testNodeByPath();
	void testNodeByPathNS();
	
	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ElementTest_INCLUDED
