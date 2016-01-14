//
// DocumentTypeTest.cpp
//
// $Id: //poco/1.4/XML/testsuite/src/DocumentTypeTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DocumentTypeTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/DOM/DocumentType.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Notation.h"
#include "Poco/DOM/Entity.h"
#include "Poco/DOM/DOMImplementation.h"
#include "Poco/DOM/NamedNodeMap.h"
#include "Poco/DOM/AutoPtr.h"


using Poco::XML::DocumentType;
using Poco::XML::Document;
using Poco::XML::Entity;
using Poco::XML::Notation;
using Poco::XML::DOMImplementation;
using Poco::XML::NamedNodeMap;
using Poco::XML::AutoPtr;


DocumentTypeTest::DocumentTypeTest(const std::string& name): CppUnit::TestCase(name)
{
}


DocumentTypeTest::~DocumentTypeTest()
{
}


void DocumentTypeTest::testDocumentType()
{
	AutoPtr<DocumentType> pDoctype = DOMImplementation::instance().createDocumentType("test", "public", "system");
	
	assert (pDoctype->ownerDocument() == 0);
	assert (pDoctype->name() == "test");
	assert (pDoctype->publicId() == "public");
	assert (pDoctype->systemId() == "system");
	
	AutoPtr<Document> pDoc = new Document(pDoctype);
	assert (pDoc->doctype() == pDoctype);
	assert (pDoctype->ownerDocument() == pDoc);

	AutoPtr<NamedNodeMap> pEntities = pDoctype->entities();
	AutoPtr<NamedNodeMap> pNotations = pDoctype->notations();
	
	assert (pEntities->length() == 0);
	assert (pNotations->length() == 0);
	
	AutoPtr<Entity> pEntity1 = pDoc->createEntity("entity1", "public1", "system1", "");
	pDoctype->appendChild(pEntity1);
	
	assert (pEntities->length() == 1);
	assert (pNotations->length() == 0);
	assert (pEntities->item(0) == pEntity1);
	assert (pEntities->getNamedItem("entity1") == pEntity1);

	AutoPtr<Entity> pEntity2 = pDoc->createEntity("entity2", "public2", "system2", "");
	pDoctype->appendChild(pEntity2);
	assert (pEntities->length() == 2);
	assert (pNotations->length() == 0);
	assert (pEntities->item(0) == pEntity1);
	assert (pEntities->item(1) == pEntity2);
	assert (pEntities->getNamedItem("entity1") == pEntity1);
	assert (pEntities->getNamedItem("entity2") == pEntity2);
	
	AutoPtr<Notation> pNotation = pDoc->createNotation("notation", "public", "system");
	pDoctype->appendChild(pNotation);
	assert (pEntities->length() == 2);
	assert (pNotations->length() == 1);
	assert (pEntities->item(0) == pEntity1);
	assert (pEntities->item(1) == pEntity2);
	assert (pNotations->item(0) == pNotation);
	assert (pEntities->getNamedItem("entity1") == pEntity1);
	assert (pEntities->getNamedItem("entity2") == pEntity2);
	assert (pNotations->getNamedItem("notation") == pNotation);
}


void DocumentTypeTest::setUp()
{
}


void DocumentTypeTest::tearDown()
{
}


CppUnit::Test* DocumentTypeTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DocumentTypeTest");

	CppUnit_addTest(pSuite, DocumentTypeTest, testDocumentType);

	return pSuite;
}
