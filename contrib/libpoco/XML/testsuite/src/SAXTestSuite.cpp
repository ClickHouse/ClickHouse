//
// SAXTestSuite.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/SAXTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SAXTestSuite.h"
#include "AttributesImplTest.h"
#include "NamespaceSupportTest.h"
#include "SAXParserTest.h"


CppUnit::Test* SAXTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("SAXTestSuite");

	pSuite->addTest(AttributesImplTest::suite());
	pSuite->addTest(NamespaceSupportTest::suite());
	pSuite->addTest(SAXParserTest::suite());

	return pSuite;
}
