//
// TreeWalker.cpp
//
// Library: XML
// Package: DOM
// Module:  TreeWalker
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/TreeWalker.h"
#include "Poco/DOM/Node.h"
#include "Poco/DOM/NodeFilter.h"


namespace Poco {
namespace XML {


TreeWalker::TreeWalker(Node* root, unsigned long whatToShow, NodeFilter* pFilter):
	_pRoot(root),
	_whatToShow(whatToShow),
	_pFilter(pFilter),
	_pCurrent(root)
{
}

	
TreeWalker::TreeWalker(const TreeWalker& walker):
	_pRoot(walker._pRoot),
	_whatToShow(walker._whatToShow),
	_pFilter(walker._pFilter),
	_pCurrent(walker._pCurrent)
{
}

	
TreeWalker& TreeWalker::operator = (const TreeWalker& walker)
{
	if (&walker != this)
	{
		_pRoot      = walker._pRoot;
		_whatToShow = walker._whatToShow;
		_pFilter    = walker._pFilter;
		_pCurrent   = walker._pCurrent;
	}
	return *this;
}

	
TreeWalker::~TreeWalker()
{
}


void TreeWalker::setCurrentNode(Node* pNode)
{
	_pCurrent = pNode;
}


Node* TreeWalker::parentNode()
{
	if (!_pCurrent || _pCurrent == _pRoot) return 0;
	
	Node* pParent = _pCurrent->parentNode();
	while (pParent && pParent != _pRoot && accept(pParent) != NodeFilter::FILTER_ACCEPT)
		pParent = pParent->parentNode();
	if (pParent && accept(pParent) == NodeFilter::FILTER_ACCEPT)
		_pCurrent = pParent;
	else
		pParent = 0;
	return pParent;
}


Node* TreeWalker::firstChild()
{
	if (!_pCurrent) return 0;

	Node* pNode = accept(_pCurrent) != NodeFilter::FILTER_REJECT ? _pCurrent->firstChild() : 0;
	while (pNode && accept(pNode) != NodeFilter::FILTER_ACCEPT)
		pNode = pNode->nextSibling();
	if (pNode)
		_pCurrent = pNode;
	return pNode;
}


Node* TreeWalker::lastChild()
{
	if (!_pCurrent) return 0;

	Node* pNode = accept(_pCurrent) != NodeFilter::FILTER_REJECT ? _pCurrent->lastChild() : 0;
	while (pNode && accept(pNode) != NodeFilter::FILTER_ACCEPT)
		pNode = pNode->previousSibling();
	if (pNode)
		_pCurrent = pNode;
	return pNode;
}


Node* TreeWalker::previousSibling()
{
	if (!_pCurrent) return 0;

	Node* pNode = _pCurrent->previousSibling();
	while (pNode && accept(pNode) != NodeFilter::FILTER_ACCEPT)
		pNode = pNode->previousSibling();
	if (pNode)
		_pCurrent = pNode;
	return pNode;
}


Node* TreeWalker::nextSibling()
{
	if (!_pCurrent) return 0;

	Node* pNode = _pCurrent->nextSibling();
	while (pNode && accept(pNode) != NodeFilter::FILTER_ACCEPT)
		pNode = pNode->nextSibling();
	if (pNode)
		_pCurrent = pNode;
	return pNode;
}


Node* TreeWalker::previousNode()
{
	if (!_pCurrent) return 0;
	
	Node* pPrev = previous(_pCurrent);
	while (pPrev && accept(pPrev) != NodeFilter::FILTER_ACCEPT)
		pPrev = previous(pPrev);
	if (pPrev)
		_pCurrent = pPrev;
	return pPrev;
}


Node* TreeWalker::nextNode()
{
	if (!_pCurrent) return 0;

	Node* pNext = next(_pCurrent);
	while (pNext && accept(pNext) != NodeFilter::FILTER_ACCEPT)
		pNext = next(pNext);
	if (pNext)
		_pCurrent = pNext;
	return pNext;
}


int TreeWalker::accept(Node* pNode) const
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
		return _pFilter->acceptNode(pNode);
	else
		return accept ? NodeFilter::FILTER_ACCEPT : NodeFilter::FILTER_REJECT;
}


Node* TreeWalker::next(Node* pNode) const
{
	Node* pNext = accept(pNode) != NodeFilter::FILTER_REJECT ? pNode->firstChild() : 0;
	if (pNext) return pNext;
	pNext = pNode;
	while (pNext && pNext != _pRoot)
	{
		Node* pSibling = pNext->nextSibling();
		if (pSibling) return pSibling;
		pNext = pNext->parentNode();
	}
	return 0;
}


Node* TreeWalker::previous(Node* pNode) const
{
	if (pNode == _pRoot) return 0;
	Node* pPrev = pNode->previousSibling();
	while (pPrev)
	{
		Node* pLastChild = accept(pPrev) != NodeFilter::FILTER_REJECT ? pPrev->lastChild() : 0;
		if (pLastChild)
			pPrev = pLastChild;
		else
			return pPrev;
	}
	return pNode->parentNode();
}


} } // namespace Poco::XML
