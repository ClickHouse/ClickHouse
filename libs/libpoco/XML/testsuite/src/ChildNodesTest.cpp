//
// ChildNodesTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/ChildNodesTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ChildNodesTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/NodeList.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::Element;
using Poco::XML::Document;
using Poco::XML::NodeList;
using Poco::XML::Node;
using Poco::XML::AutoPtr;


ChildNodesTest::ChildNodesTest(const std::string& name): CppUnit::TestCase(name)
{
}


ChildNodesTest::~ChildNodesTest()
{
}


void ChildNodesTest::testChildNodes()
{
	AutoPtr<Document> pDoc = new Document;
	AutoPtr<Element> pRoot = pDoc->createElement("root");

	assert (!pRoot->hasChildNodes());
	AutoPtr<NodeList> pNL = pRoot->childNodes();
	assert (pNL->length() == 0);
	
	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	pRoot->appendChild(pChild1);
	assert (pRoot->hasChildNodes());
	
	assert (pNL->length() == 1);
	assert (pNL->item(0) == pChild1);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	pRoot->appendChild(pChild2);

	assert (pNL->length() == 2);
	assert (pNL->item(0) == pChild1);
	assert (pNL->item(1) == pChild2);
	
	AutoPtr<Element> pChild0 = pDoc->createElement("child0");
	pRoot->insertBefore(pChild0, pChild1);

	assert (pNL->length() == 3);
	assert (pNL->item(0) == pChild0);
	assert (pNL->item(1) == pChild1);
	assert (pNL->item(2) == pChild2);

	pRoot->removeChild(pChild1);
	assert (pNL->length() == 2);
	assert (pNL->item(0) == pChild0);
	assert (pNL->item(1) == pChild2);

	pRoot->removeChild(pChild0);
	assert (pNL->length() == 1);
	assert (pNL->item(0) == pChild2);

	pRoot->removeChild(pChild2);
	assert (pNL->length() == 0);
	assert (pNL->item(0) == 0);

	assert (!pRoot->hasChildNodes());
}


void ChildNodesTest::setUp()
{
}


void ChildNodesTest::tearDown()
{
}


CppUnit::Test* ChildNodesTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ChildNodesTest");

	CppUnit_addTest(pSuite, ChildNodesTest, testChildNodes);

	return pSuite;
}
