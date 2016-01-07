//
// DocumentFragment.cpp
//
// $Id: //poco/1.4/XML/src/DocumentFragment.cpp#1 $
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


#include "Poco/DOM/DocumentFragment.h"


namespace Poco {
namespace XML {


const XMLString DocumentFragment::NODE_NAME = toXMLString("#document-fragment");


DocumentFragment::DocumentFragment(Document* pOwnerDocument): 
	AbstractContainerNode(pOwnerDocument)
{
}


DocumentFragment::DocumentFragment( Document* pOwnerDocument, const DocumentFragment& fragment): 
	AbstractContainerNode(pOwnerDocument, fragment)
{
}


DocumentFragment::~DocumentFragment()
{
}


const XMLString& DocumentFragment::nodeName() const
{
	return NODE_NAME;
}


unsigned short DocumentFragment::nodeType() const
{
	return Node::DOCUMENT_FRAGMENT_NODE;
}


Node* DocumentFragment::copyNode(bool deep, Document* pOwnerDocument) const
{
	DocumentFragment* pClone = new DocumentFragment(pOwnerDocument, *this);
	if (deep)
	{
		Node* pCur = firstChild();
		while (pCur)
		{
			pClone->appendChild(static_cast<AbstractNode*>(pCur)->copyNode(deep, pOwnerDocument))->release();
			pCur = pCur->nextSibling();
		}
	}
	return pClone;
}


} } // namespace Poco::XML
