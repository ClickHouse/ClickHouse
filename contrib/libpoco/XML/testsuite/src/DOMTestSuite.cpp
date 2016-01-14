//
// DOMTestSuite.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/DOMTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DOMTestSuite.h"
#include "NodeTest.h"
#include "ChildNodesTest.h"
#include "ElementTest.h"
#include "TextTest.h"
#include "DocumentTest.h"
#include "DocumentTypeTest.h"
#include "EventTest.h"
#include "NodeIteratorTest.h"
#include "TreeWalkerTest.h"
#include "ParserWriterTest.h"
#include "NodeAppenderTest.h"


CppUnit::Test* DOMTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DOMTestSuite");

	pSuite->addTest(NodeTest::suite());
	pSuite->addTest(ChildNodesTest::suite());
	pSuite->addTest(ElementTest::suite());
	pSuite->addTest(TextTest::suite());
	pSuite->addTest(DocumentTest::suite());
	pSuite->addTest(DocumentTypeTest::suite());
	pSuite->addTest(EventTest::suite());
	pSuite->addTest(NodeIteratorTest::suite());
	pSuite->addTest(TreeWalkerTest::suite());
	pSuite->addTest(ParserWriterTest::suite());
	pSuite->addTest(NodeAppenderTest::suite());

	return pSuite;
}
