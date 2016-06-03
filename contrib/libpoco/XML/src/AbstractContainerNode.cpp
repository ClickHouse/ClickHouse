//
// AbstractContainerNode.cpp
//
// $Id: //poco/1.4/XML/src/AbstractContainerNode.cpp#2 $
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


#include "Poco/DOM/AbstractContainerNode.h"
#include "Poco/DOM/Document.h"
#include "Poco/DOM/Element.h"
#include "Poco/DOM/Attr.h"
#include "Poco/DOM/DOMException.h"
#include "Poco/DOM/ElementsByTagNameList.h"
#include "Poco/DOM/AutoPtr.h"
#include "Poco/NumberParser.h"
#include "Poco/UnicodeConverter.h"


namespace Poco {
namespace XML {


AbstractContainerNode::AbstractContainerNode(Document* pOwnerDocument): 
	AbstractNode(pOwnerDocument),
	_pFirstChild(0)
{
}


AbstractContainerNode::AbstractContainerNode(Document* pOwnerDocument, const AbstractContainerNode& node): 
	AbstractNode(pOwnerDocument, node),
	_pFirstChild(0)
{
}


AbstractContainerNode::~AbstractContainerNode()
{
	AbstractNode* pChild = static_cast<AbstractNode*>(_pFirstChild);
	while (pChild)
	{
		AbstractNode* pDelNode = pChild;
		pChild = pChild->_pNext;
		pDelNode->_pNext   = 0;
		pDelNode->_pParent = 0;
		pDelNode->release();
	}
}


Node* AbstractContainerNode::firstChild() const
{
	return _pFirstChild;
}


Node* AbstractContainerNode::lastChild() const
{
	AbstractNode* pChild = _pFirstChild;
	if (pChild)
	{
		while (pChild->_pNext) pChild = pChild->_pNext;
		return pChild;
	}
	return 0;
}


Node* AbstractContainerNode::insertBefore(Node* newChild, Node* refChild)
{
	poco_check_ptr (newChild);

	if (static_cast<AbstractNode*>(newChild)->_pOwner != _pOwner && static_cast<AbstractNode*>(newChild)->_pOwner != this)
		throw DOMException(DOMException::WRONG_DOCUMENT_ERR);
	if (refChild && static_cast<AbstractNode*>(refChild)->_pParent != this)
		throw DOMException(DOMException::NOT_FOUND_ERR);
	if (newChild == refChild)
		return newChild;
	if (this == newChild)
		throw DOMException(DOMException::HIERARCHY_REQUEST_ERR);

	AbstractNode* pFirst = 0;
	AbstractNode* pLast  = 0;
	if (newChild->nodeType() == Node::DOCUMENT_FRAGMENT_NODE)
	{
		AbstractContainerNode* pFrag = static_cast<AbstractContainerNode*>(newChild);
		pFirst = pFrag->_pFirstChild;
		pLast  = pFirst;
		if (pFirst)
		{
			while (pLast->_pNext)
			{
				pLast->_pParent = this;
				pLast = pLast->_pNext;
			}
			pLast->_pParent = this;
		}
		pFrag->_pFirstChild = 0;
	}
	else
	{
		newChild->duplicate();
		AbstractContainerNode* pParent = static_cast<AbstractNode*>(newChild)->_pParent;
		if (pParent) pParent->removeChild(newChild);
		pFirst = static_cast<AbstractNode*>(newChild);
		pLast  = pFirst;
		pFirst->_pParent = this;
	}
	if (_pFirstChild && pFirst)
	{
		AbstractNode* pCur = _pFirstChild;
		if (pCur == refChild)
		{
			pLast->_pNext = _pFirstChild;
			_pFirstChild  = pFirst;
		}
		else
		{
			while (pCur && pCur->_pNext != refChild) pCur = pCur->_pNext;
			if (pCur)
			{
				pLast->_pNext = pCur->_pNext;
				pCur->_pNext = pFirst;
			}
			else throw DOMException(DOMException::NOT_FOUND_ERR);
		}
	}
	else _pFirstChild = pFirst;

	if (events())
	{
		while (pFirst && pFirst != pLast->_pNext)
		{
			pFirst->dispatchNodeInserted();
			pFirst->dispatchNodeInsertedIntoDocument();
			pFirst = pFirst->_pNext;
		}
		dispatchSubtreeModified();
	}
	return newChild;
}


Node* AbstractContainerNode::replaceChild(Node* newChild, Node* oldChild)
{
	poco_check_ptr (newChild);
	poco_check_ptr (oldChild);

	if (static_cast<AbstractNode*>(newChild)->_pOwner != _pOwner && static_cast<AbstractNode*>(newChild)->_pOwner != this)
		throw DOMException(DOMException::WRONG_DOCUMENT_ERR);
	if (static_cast<AbstractNode*>(oldChild)->_pParent != this)
		throw DOMException(DOMException::NOT_FOUND_ERR);
	if (newChild == oldChild)
		return newChild;
	if (this == newChild)
		throw DOMException(DOMException::HIERARCHY_REQUEST_ERR);

	bool doEvents = events();
	if (newChild->nodeType() == Node::DOCUMENT_FRAGMENT_NODE)
	{
		insertBefore(newChild, oldChild);
		removeChild(oldChild);
	}
	else
	{
		AbstractContainerNode* pParent = static_cast<AbstractNode*>(newChild)->_pParent;
		if (pParent) pParent->removeChild(newChild);

		if (oldChild == _pFirstChild)
		{
			if (doEvents)
			{
				_pFirstChild->dispatchNodeRemoved();
				_pFirstChild->dispatchNodeRemovedFromDocument();
			}
			static_cast<AbstractNode*>(newChild)->_pNext   = static_cast<AbstractNode*>(oldChild)->_pNext;
			static_cast<AbstractNode*>(newChild)->_pParent = this;
			_pFirstChild->_pNext   = 0;
			_pFirstChild->_pParent = 0;
			_pFirstChild = static_cast<AbstractNode*>(newChild);
			if (doEvents)
			{
				static_cast<AbstractNode*>(newChild)->dispatchNodeInserted();
				static_cast<AbstractNode*>(newChild)->dispatchNodeInsertedIntoDocument();
			}
		}
		else
		{
			AbstractNode* pCur = _pFirstChild;
			while (pCur && pCur->_pNext != oldChild) pCur = pCur->_pNext;
			if (pCur)
			{	
				poco_assert_dbg (pCur->_pNext == oldChild);

				if (doEvents)
				{
					static_cast<AbstractNode*>(oldChild)->dispatchNodeRemoved();
					static_cast<AbstractNode*>(oldChild)->dispatchNodeRemovedFromDocument();
				}
				static_cast<AbstractNode*>(newChild)->_pNext   = static_cast<AbstractNode*>(oldChild)->_pNext;
				static_cast<AbstractNode*>(newChild)->_pParent = this;
				static_cast<AbstractNode*>(oldChild)->_pNext   = 0;
				static_cast<AbstractNode*>(oldChild)->_pParent = 0;
				pCur->_pNext = static_cast<AbstractNode*>(newChild);
				if (doEvents)
				{
					static_cast<AbstractNode*>(newChild)->dispatchNodeInserted();
					static_cast<AbstractNode*>(newChild)->dispatchNodeInsertedIntoDocument();
				}
			}
			else throw DOMException(DOMException::NOT_FOUND_ERR);
		}
		newChild->duplicate();
		oldChild->autoRelease();
	}
	if (doEvents) dispatchSubtreeModified();
	return oldChild;
}


Node* AbstractContainerNode::removeChild(Node* oldChild)
{
	poco_check_ptr (oldChild);

	bool doEvents = events();
	if (oldChild == _pFirstChild)
	{
		if (doEvents)
		{
			static_cast<AbstractNode*>(oldChild)->dispatchNodeRemoved();
			static_cast<AbstractNode*>(oldChild)->dispatchNodeRemovedFromDocument();
		}
		_pFirstChild = _pFirstChild->_pNext;
		static_cast<AbstractNode*>(oldChild)->_pNext   = 0;
		static_cast<AbstractNode*>(oldChild)->_pParent = 0;
	}
	else
	{
		AbstractNode* pCur = _pFirstChild;
		while (pCur && pCur->_pNext != oldChild) pCur = pCur->_pNext;
		if (pCur)
		{
			if (doEvents)
			{
				static_cast<AbstractNode*>(oldChild)->dispatchNodeRemoved();
				static_cast<AbstractNode*>(oldChild)->dispatchNodeRemovedFromDocument();
			}
			pCur->_pNext = pCur->_pNext->_pNext;
			static_cast<AbstractNode*>(oldChild)->_pNext   = 0;
			static_cast<AbstractNode*>(oldChild)->_pParent = 0;
		}
		else throw DOMException(DOMException::NOT_FOUND_ERR);
	}
	oldChild->autoRelease();
	if (doEvents) dispatchSubtreeModified();
	return oldChild;
}


Node* AbstractContainerNode::appendChild(Node* newChild)
{
	return insertBefore(newChild, 0);
}


void AbstractContainerNode::dispatchNodeRemovedFromDocument()
{
	AbstractNode::dispatchNodeRemovedFromDocument();
	Node* pChild = firstChild();
	while (pChild)
	{
		static_cast<AbstractNode*>(pChild)->dispatchNodeRemovedFromDocument();
		pChild = pChild->nextSibling();
	}
}


void AbstractContainerNode::dispatchNodeInsertedIntoDocument()
{
	AbstractNode::dispatchNodeInsertedIntoDocument();
	Node* pChild = firstChild();
	while (pChild)
	{
		static_cast<AbstractNode*>(pChild)->dispatchNodeInsertedIntoDocument();
		pChild = pChild->nextSibling();
	}
}


bool AbstractContainerNode::hasChildNodes() const
{
	return _pFirstChild != 0;
}


bool AbstractContainerNode::hasAttributes() const
{
	return false;
}


Node* AbstractContainerNode::getNodeByPath(const XMLString& path) const
{
	XMLString::const_iterator it = path.begin();
	if (it != path.end() && *it == '/') 
	{
		++it;
		if (it != path.end() && *it == '/')
		{
			++it;
			XMLString name;
			while (it != path.end() && *it != '/' && *it != '@' && *it != '[') name += *it++;
			if (it != path.end() && *it == '/') ++it;
			if (name.empty()) name += '*';
			AutoPtr<ElementsByTagNameList> pList = new ElementsByTagNameList(this, name);
			unsigned long length = pList->length();
			for (unsigned long i = 0; i < length; i++)
			{
				XMLString::const_iterator beg = it;
				const Node* pNode = findNode(beg, path.end(), pList->item(i), 0);
				if (pNode) return const_cast<Node*>(pNode);
			}
			return 0;
		}
	}
	return const_cast<Node*>(findNode(it, path.end(), this, 0));
}


Node* AbstractContainerNode::getNodeByPathNS(const XMLString& path, const NSMap& nsMap) const
{
	XMLString::const_iterator it = path.begin();
	if (it != path.end() && *it == '/') 
	{
		++it;
		if (it != path.end() && *it == '/')
		{
			++it;
			XMLString name;
			while (it != path.end() && *it != '/' && *it != '@' && *it != '[') name += *it++;
			if (it != path.end() && *it == '/') ++it;
			XMLString namespaceURI;
			XMLString localName;
			bool nameOK = true;
			if (name.empty())
			{
				namespaceURI += '*';
				localName += '*';
			}
			else
			{
				nameOK = nsMap.processName(name, namespaceURI, localName, false);
			}
			if (nameOK)
			{
				AutoPtr<ElementsByTagNameListNS> pList = new ElementsByTagNameListNS(this, namespaceURI, localName);
				unsigned long length = pList->length();
				for (unsigned long i = 0; i < length; i++)
				{
					XMLString::const_iterator beg = it;
					const Node* pNode = findNode(beg, path.end(), pList->item(i), &nsMap);
					if (pNode) return const_cast<Node*>(pNode);
				}
			}
			return 0;
		}
	}
	return const_cast<Node*>(findNode(it, path.end(), this, &nsMap));
}


const Node* AbstractContainerNode::findNode(XMLString::const_iterator& it, const XMLString::const_iterator& end, const Node* pNode, const NSMap* pNSMap)
{
	if (pNode && it != end)
	{
		if (*it == '[')
		{
			++it;
			if (it != end && *it == '@')
			{
				++it;
				XMLString attr;
				while (it != end && *it != ']' && *it != '=') attr += *it++;
				if (it != end && *it == '=')
				{
					++it;
					XMLString value;
					if (it != end && *it == '\'')
					{
						++it;
						while (it != end && *it != '\'') value += *it++;
						if (it != end) ++it;
					}
					else
					{
						while (it != end && *it != ']') value += *it++;
					}
					if (it != end) ++it;
					return findNode(it, end, findElement(attr, value, pNode, pNSMap), pNSMap);
				}
				else
				{
					if (it != end) ++it;
					return findAttribute(attr, pNode, pNSMap);
				}
			}
			else
			{
				XMLString index;
				while (it != end && *it != ']') index += *it++;
				if (it != end) ++it;
#ifdef XML_UNICODE_WCHAR_T
				std::string idx;
				Poco::UnicodeConverter::convert(index, idx);
				return findNode(it, end, findElement(Poco::NumberParser::parse(idx), pNode, pNSMap), pNSMap);
#else
				return findNode(it, end, findElement(Poco::NumberParser::parse(index), pNode, pNSMap), pNSMap);
#endif
			}
		}
		else
		{
			while (it != end && *it == '/') ++it;
			XMLString key;
			while (it != end && *it != '/' && *it != '[') key += *it++;
			return findNode(it, end, findElement(key, pNode, pNSMap), pNSMap);
		}
	}
	else return pNode;
}


const Node* AbstractContainerNode::findElement(const XMLString& name, const Node* pNode, const NSMap* pNSMap)
{
	Node* pChild = pNode->firstChild();
	while (pChild)
	{
		if (pChild->nodeType() == Node::ELEMENT_NODE && namesAreEqual(pChild, name, pNSMap))
			return pChild;
		pChild = pChild->nextSibling();
	}
	return 0;
}


const Node* AbstractContainerNode::findElement(int index, const Node* pNode, const NSMap* pNSMap)
{
	const Node* pRefNode = pNode;
	if (index > 0)
	{
		pNode = pNode->nextSibling();
		while (pNode)
		{
			if (namesAreEqual(pNode, pRefNode, pNSMap))
			{
				if (--index == 0) break;
			}
			pNode = pNode->nextSibling();
		}
	}
	return pNode;
}


const Node* AbstractContainerNode::findElement(const XMLString& attr, const XMLString& value, const Node* pNode, const NSMap* pNSMap)
{
	const Node* pRefNode = pNode;
	const Element* pElem = dynamic_cast<const Element*>(pNode);
	if (!(pElem && pElem->hasAttributeValue(attr, value, pNSMap)))
	{
		pNode = pNode->nextSibling();
		while (pNode)
		{
			if (namesAreEqual(pNode, pRefNode, pNSMap))
			{
				pElem = dynamic_cast<const Element*>(pNode);
				if (pElem && pElem->hasAttributeValue(attr, value, pNSMap)) break;
			}
			pNode = pNode->nextSibling();
		}
	}
	return pNode;
}


const Attr* AbstractContainerNode::findAttribute(const XMLString& name, const Node* pNode, const NSMap* pNSMap)
{
	const Attr* pResult(0);
	const Element* pElem = dynamic_cast<const Element*>(pNode);
	if (pElem)
	{
		if (pNSMap)
		{
			XMLString namespaceURI;
			XMLString localName;
			if (pNSMap->processName(name, namespaceURI, localName, true))
			{
				pResult = pElem->getAttributeNodeNS(namespaceURI, localName);
			}
		}
		else
		{
			pResult = pElem->getAttributeNode(name);
		}
	}
	return pResult;
}


bool AbstractContainerNode::hasAttributeValue(const XMLString& name, const XMLString& value, const NSMap* pNSMap) const
{
	const Attr* pAttr = findAttribute(name, this, pNSMap);
	return pAttr && pAttr->getValue() == value;
}


bool AbstractContainerNode::namesAreEqual(const Node* pNode1, const Node* pNode2, const NSMap* pNSMap)
{
	if (pNSMap)
	{
		return pNode1->localName() == pNode2->localName() && pNode1->namespaceURI() == pNode2->namespaceURI();
	}
	else
	{
		return pNode1->nodeName() == pNode2->nodeName();
	}
}


bool AbstractContainerNode::namesAreEqual(const Node* pNode, const XMLString& name, const NSMap* pNSMap)
{
	if (pNSMap)
	{
		XMLString namespaceURI;
		XMLString localName;
		if (pNSMap->processName(name, namespaceURI, localName, false))
		{
			return pNode->namespaceURI() == namespaceURI && pNode->localName() == localName;
		}
		else return false;
	}
	else
	{
		return pNode->nodeName() == name;
	}
}


} } // namespace Poco::XML
