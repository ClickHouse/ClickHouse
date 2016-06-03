//
// Text.cpp
//
// $Id: //poco/1.4/XML/src/Text.cpp#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/Text.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/DOMException.h"


namespace Poco {
namespace XML {


const XMLString Text::NODE_NAME = toXMLString("#text");


Text::Text(Document* pOwnerDocument, const XMLString& data): 
	CharacterData(pOwnerDocument, data)
{
}


Text::Text(Document* pOwnerDocument, const Text& text): 
	CharacterData(pOwnerDocument, text)
{
}


Text::~Text()
{
}


Text* Text::splitText(unsigned long offset)
{
	Node* pParent = parentNode();
	if (!pParent) throw DOMException(DOMException::HIERARCHY_REQUEST_ERR);
	int n = length() - offset;
	Text* pNew = ownerDocument()->createTextNode(substringData(offset, n));
	deleteData(offset, n);
	pParent->insertBefore(pNew, nextSibling())->release();
	return pNew;
}


const XMLString& Text::nodeName() const
{
	return NODE_NAME;
}


unsigned short Text::nodeType() const
{
	return Node::TEXT_NODE;
}


XMLString Text::innerText() const
{
	return nodeValue();
}


Node* Text::copyNode(bool deep, Document* pOwnerDocument) const
{
	return new Text(pOwnerDocument, *this);
}


} } // namespace Poco::XML
