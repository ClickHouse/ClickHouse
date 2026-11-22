//
// ElementsByTagNameList.cpp
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


#include "Poco/DOM/ElementsByTagNameList.h"
#include "Poco/DOM/Node.h"
#include "Poco/DOM/Document.h"
#include <climits>


namespace Poco {
namespace XML {


ElementsByTagNameList::ElementsByTagNameList(const Node* pParent, const XMLString& name):
	_pParent(pParent),
	_name(name),
	_count(0)
{
	poco_check_ptr (pParent);
	
	_pParent->duplicate();
}


ElementsByTagNameList::~ElementsByTagNameList()
{
	_pParent->release();
}


Node* ElementsByTagNameList::item(unsigned long index) const
{
	_count = 0;
	return find(_pParent, index);
}


unsigned long ElementsByTagNameList::length() const
{
	_count = 0;
	find(_pParent, ULONG_MAX);
	return _count;
}


namespace
{
	static const XMLString asterisk = toXMLString("*");
}


Node* ElementsByTagNameList::find(const Node* pParent, unsigned long index) const
{
	if (!pParent) return 0;

	// preorder search
	Node* pCur = pParent->firstChild();
	while (pCur)
	{
		if (pCur->nodeType() == Node::ELEMENT_NODE && (_name == asterisk || pCur->nodeName() == _name))
		{
			if (_count == index) return pCur;
			_count++;
		}
		Node* pNode = find(pCur, index);
		if (pNode) return pNode;
		pCur = pCur->nextSibling();
	}
	return pCur;
}


void ElementsByTagNameList::autoRelease()
{
	_pParent->ownerDocument()->autoReleasePool().add(this);
}


ElementsByTagNameListNS::ElementsByTagNameListNS(const Node* pParent, const XMLString& namespaceURI, const XMLString& localName):
	_pParent(pParent),
	_localName(localName),
	_namespaceURI(namespaceURI),
	_count(0)
{
	poco_check_ptr (pParent);
	
	_pParent->duplicate();
}



ElementsByTagNameListNS::~ElementsByTagNameListNS()
{
	_pParent->release();
}


Node* ElementsByTagNameListNS::item(unsigned long index) const
{
	_count = 0;
	return find(_pParent, index);
}


unsigned long ElementsByTagNameListNS::length() const
{
	_count = 0;
	find(_pParent, ULONG_MAX);
	return _count;
}


Node* ElementsByTagNameListNS::find(const Node* pParent, unsigned long index) const
{
	if (!pParent) return 0;

	// preorder search
	Node* pCur = pParent->firstChild();
	while (pCur)
	{
		if (pCur->nodeType() == Node::ELEMENT_NODE && (_localName == asterisk || pCur->localName() == _localName) && (_namespaceURI == asterisk || pCur->namespaceURI() == _namespaceURI))
		{
			if (_count == index) return pCur;
			_count++;
		}
		Node* pNode = find(pCur, index);
		if (pNode) return pNode;
		pCur = pCur->nextSibling();
	}
	return pCur;
}


void ElementsByTagNameListNS::autoRelease()
{
	_pParent->ownerDocument()->autoReleasePool().add(this);
}


} } // namespace Poco::XML
