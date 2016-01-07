//
// DTDMap.cpp
//
// $Id: //poco/1.4/XML/src/DTDMap.cpp#1 $
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


#include "Poco/DOM/DTDMap.h"
#include "Poco/DOM/DocumentType.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/DOMException.h"


namespace Poco {
namespace XML {


DTDMap::DTDMap(const DocumentType* pDocumentType, unsigned short type):
	_pDocumentType(pDocumentType),
	_type(type)
{
	poco_check_ptr (pDocumentType->ownerDocument());
}


DTDMap::~DTDMap()
{
}


Node* DTDMap::getNamedItem(const XMLString& name) const
{
	Node* pCur = _pDocumentType->firstChild();
	while (pCur)
	{
		if (pCur->nodeType() == _type && pCur->nodeName() == name)
			return pCur;
		pCur = pCur->nextSibling();
	}
	return pCur;
}


Node* DTDMap::setNamedItem(Node* arg)
{
	throw DOMException(DOMException::NO_MODIFICATION_ALLOWED_ERR);
}


Node* DTDMap::removeNamedItem(const XMLString& name)
{
	throw DOMException(DOMException::NO_MODIFICATION_ALLOWED_ERR);
}


Node* DTDMap::item(unsigned long index) const
{
	unsigned long n = 0;
	Node* pCur = _pDocumentType->firstChild();
	while (pCur)
	{
		if (pCur->nodeType() == _type)
		{
			if (n == index) return pCur;
			++n;
		}
		pCur = pCur->nextSibling();
	}
	return pCur;
}


unsigned long DTDMap::length() const
{
	unsigned long n = 0;
	Node* pCur = _pDocumentType->firstChild();
	while (pCur)
	{
		if (pCur->nodeType() == _type) ++n;
		pCur = pCur->nextSibling();
	}
	return n;
}


Node* DTDMap::getNamedItemNS(const XMLString& namespaceURI, const XMLString& localName) const
{
	return 0;
}


Node* DTDMap::setNamedItemNS(Node* arg)
{
	throw DOMException(DOMException::NO_MODIFICATION_ALLOWED_ERR);
}


Node* DTDMap::removeNamedItemNS(const XMLString& namespaceURI, const XMLString& localName)
{
	throw DOMException(DOMException::NO_MODIFICATION_ALLOWED_ERR);
}


void DTDMap::autoRelease()
{
	_pDocumentType->ownerDocument()->autoReleasePool().add(this);
}


} } // namespace Poco::XML
