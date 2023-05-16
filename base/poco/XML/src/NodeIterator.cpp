//
// NodeIterator.cpp
//
// Library: XML
// Package: DOM
// Module:  NodeIterator
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/NodeIterator.h"
#include "Poco/DOM/AbstractNode.h"
#include "Poco/DOM/NodeFilter.h"
#include "Poco/DOM/DOMException.h"


namespace Poco {
namespace XML {


NodeIterator::NodeIterator(Node* root, unsigned long whatToShow, NodeFilter* pFilter):
	_pRoot(root),
	_whatToShow(whatToShow),
	_pFilter(pFilter),
	_pCurrent(0)
{
}

	
NodeIterator::NodeIterator(const NodeIterator& iterator):
	_pRoot(iterator._pRoot),
	_whatToShow(iterator._whatToShow),
	_pFilter(iterator._pFilter),
	_pCurrent(iterator._pCurrent)
{
}

	
NodeIterator& NodeIterator::operator = (const NodeIterator& iterator)
{
	if (&iterator != this)
	{
		_pRoot      = iterator._pRoot;
		_whatToShow = iterator._whatToShow;
		_pFilter    = iterator._pFilter;
		_pCurrent   = iterator._pCurrent;
	}
	return *this;
}

	
NodeIterator::~NodeIterator()
{
}


Node* NodeIterator::nextNode()
{
	if (!_pRoot) throw DOMException(DOMException::INVALID_STATE_ERR);
	
	if (_pCurrent)
		_pCurrent = next();
	else
		_pCurrent = _pRoot;
	while (_pCurrent && !accept(_pCurrent))
		_pCurrent = next();
	return _pCurrent;
}


Node* NodeIterator::previousNode()
{
	if (!_pRoot) throw DOMException(DOMException::INVALID_STATE_ERR);

	if (_pCurrent)
		_pCurrent = previous();
	else
		_pCurrent = last();
	while (_pCurrent && !accept(_pCurrent))
		_pCurrent = previous();
	return _pCurrent;
}


void NodeIterator::detach()
{
	_pRoot    = 0;
	_pCurrent = 0;
}


bool NodeIterator::accept(Node* pNode) const
{
	bool accept = false;
	switch (pNode->nodeType())
	{
	case Node::ELEMENT_NODE: 
		accept = (_whatToShow & NodeFilter::SHOW_ELEMENT) != 0; break;
	case Node::ATTRIBUTE_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_ATTRIBUTE) != 0; break; 
	case Node::TEXT_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_TEXT) != 0; break; 
	case Node::CDATA_SECTION_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_CDATA_SECTION) != 0; break; 
	case Node::ENTITY_REFERENCE_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_ENTITY_REFERENCE) != 0; break; 
	case Node::ENTITY_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_ENTITY) != 0; break; 
	case Node::PROCESSING_INSTRUCTION_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_PROCESSING_INSTRUCTION) != 0; break; 
	case Node::COMMENT_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_COMMENT) != 0; break; 
	case Node::DOCUMENT_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_DOCUMENT) != 0; break; 
	case Node::DOCUMENT_TYPE_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_DOCUMENT_TYPE) != 0; break; 
	case Node::DOCUMENT_FRAGMENT_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_DOCUMENT_FRAGMENT) != 0; break; 
	case Node::NOTATION_NODE:
		accept = (_whatToShow & NodeFilter::SHOW_NOTATION) != 0; break; 
	}
	if (accept && _pFilter)
		accept = _pFilter->acceptNode(pNode) == NodeFilter::FILTER_ACCEPT;
	return accept;
}


Node* NodeIterator::next() const
{
	Node* pNext = _pCurrent->firstChild();
	if (pNext) return pNext;
	pNext = _pCurrent;
	while (pNext && pNext != _pRoot)
	{
		Node* pSibling = pNext->nextSibling();
		if (pSibling) return pSibling;
		pNext = pNext->parentNode();
	}
	return 0;
}


Node* NodeIterator::previous() const
{
	if (_pCurrent == _pRoot) return 0;
	Node* pPrev = _pCurrent->previousSibling();
	while (pPrev)
	{
		Node* pLastChild = pPrev->lastChild();
		if (pLastChild)
			pPrev = pLastChild;
		else
			return pPrev;
	}
	return _pCurrent->parentNode();
}


Node* NodeIterator::last()
{
	_pCurrent = _pRoot;
	Node* pLast = 0;
	while (_pCurrent)
	{
		pLast = _pCurrent;
		_pCurrent = next();
	}
	return pLast;
}


} } // namespace Poco::XML
