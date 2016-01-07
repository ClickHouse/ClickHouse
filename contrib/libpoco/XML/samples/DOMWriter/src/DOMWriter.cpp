//
// DOMWriter.cpp
//
// $Id: //poco/1.4/XML/samples/DOMWriter/src/DOMWriter.cpp#1 $
//
// This sample demonstrates the DOMWriter class and how to
// build DOM documents in memory.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Text.h"
#include "Poco/DOM/AutoPtr.h"
#include "Poco/DOM/DOMWriter.h"
#include "Poco/XML/XMLWriter.h"
#include <iostream>


using Poco::XML::Document;
using Poco::XML::Element;
using Poco::XML::Text;
using Poco::XML::AutoPtr;
using Poco::XML::DOMWriter;
using Poco::XML::XMLWriter;


int main(int argc, char** argv)
{
	// build a DOM document and write it to standard output.

	AutoPtr<Document> pDoc = new Document;
	
	AutoPtr<Element> pRoot = pDoc->createElement("root");
	pDoc->appendChild(pRoot);

	AutoPtr<Element> pChild1 = pDoc->createElement("child1");
	AutoPtr<Text> pText1 = pDoc->createTextNode("text1");
	pChild1->appendChild(pText1);
	pRoot->appendChild(pChild1);

	AutoPtr<Element> pChild2 = pDoc->createElement("child2");
	AutoPtr<Text> pText2 = pDoc->createTextNode("text2");
	pChild2->appendChild(pText2);
	pRoot->appendChild(pChild2);
	
	DOMWriter writer;
	writer.setNewLine("\n");
	writer.setOptions(XMLWriter::PRETTY_PRINT);
	writer.writeNode(std::cout, pDoc);
	
	return 0;
}
