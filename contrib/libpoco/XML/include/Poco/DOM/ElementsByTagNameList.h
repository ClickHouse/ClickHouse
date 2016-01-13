//
// ElementsByTagNameList.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/ElementsByTagNameList.h#2 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the ElementsByTagNameList and ElementsByTagNameListNS classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_ElementsByTagNameList_INCLUDED
#define DOM_ElementsByTagNameList_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/NodeList.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API ElementsByTagNameList: public NodeList
	// This implementation of NodeList is returned
	// by Document::getElementsByTagName() and
	// Element::getElementsByTagName().
{
public:
	Node* item(unsigned long index) const;
	unsigned long length() const;
	void autoRelease();

protected:
	ElementsByTagNameList(const Node* pParent, const XMLString& name);
	~ElementsByTagNameList();

	Node* find(const Node* pParent, unsigned long index) const;

	const Node* _pParent;
	XMLString   _name;
	mutable unsigned long _count;
	
	friend class AbstractContainerNode;
	friend class Element;
	friend class Document;
};


class XML_API ElementsByTagNameListNS: public NodeList
	// This implementation of NodeList is returned
	// by Document::getElementsByTagNameNS() and
	// Element::getElementsByTagNameNS().
{
public:
	virtual Node* item(unsigned long index) const;
	virtual unsigned long length() const;
	virtual void autoRelease();

protected:
	ElementsByTagNameListNS(const Node* pParent, const XMLString& namespaceURI, const XMLString& localName);
	~ElementsByTagNameListNS();

	Node* find(const Node* pParent, unsigned long index) const;

	const Node* _pParent;
	XMLString   _localName;
	XMLString   _namespaceURI;
	mutable unsigned long _count;
	
	friend class AbstractContainerNode;
	friend class Element;
	friend class Document;
};


} } // namespace Poco::XML


#endif // DOM_ElementsByTagNameList_INCLUDED
