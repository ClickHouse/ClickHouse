//
// AbstractNode.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the AbstractNode class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_AbstractNode_INCLUDED
#define DOM_AbstractNode_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/Node.h"
#include "Poco/DOM/MutationEvent.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class AbstractContainerNode;
class Attr;
class EventDispatcher;


class XML_API AbstractNode: public Node
	/// AbstractNode provides a basic implementation
	/// of the Node interface for all types of nodes
	/// that do not contain other nodes.
{
public:
	// Node
	const XMLString& nodeName() const;
	const XMLString& getNodeValue() const;
	void setNodeValue(const XMLString& value);
	Node* parentNode() const;
	NodeList* childNodes() const;
	Node* firstChild() const;
	Node* lastChild() const;
	Node* previousSibling() const;
	Node* nextSibling() const;
	NamedNodeMap* attributes() const;
	Document* ownerDocument() const;
	Node* insertBefore(Node* newChild, Node* refChild);
	Node* replaceChild(Node* newChild, Node* oldChild);
	Node* removeChild(Node* oldChild);
	Node* appendChild(Node* newChild);
	bool hasChildNodes() const;
	Node* cloneNode(bool deep) const;
	void normalize();
	bool isSupported(const XMLString& feature, const XMLString& version) const;
	const XMLString& namespaceURI() const;
	XMLString prefix() const;
	const XMLString& localName() const;
	bool hasAttributes() const;

	// EventTarget
	void addEventListener(const XMLString& type, EventListener* listener, bool useCapture);
	void removeEventListener(const XMLString& type, EventListener* listener, bool useCapture);
	bool dispatchEvent(Event* evt);

	// Extensions
	XMLString innerText() const;
	Node* getNodeByPath(const XMLString& path) const;
	Node* getNodeByPathNS(const XMLString& path, const NSMap& nsMap) const;

	virtual void autoRelease();

protected:
	AbstractNode(Document* pOwnerDocument);
	AbstractNode(Document* pOwnerDocument, const AbstractNode& node);
	~AbstractNode();

	virtual Node* copyNode(bool deep, Document* pOwnerDocument) const = 0;

	virtual bool events() const;
	virtual bool eventsSuspended() const;
	void captureEvent(Event* evt);
	void bubbleEvent(Event* evt);
	void dispatchSubtreeModified();
	void dispatchNodeInserted();
	void dispatchNodeRemoved();
	virtual void dispatchNodeRemovedFromDocument();
	virtual void dispatchNodeInsertedIntoDocument();
	void dispatchAttrModified(Attr* pAttr, MutationEvent::AttrChangeType changeType, const XMLString& prevValue, const XMLString& newValue);
	void dispatchCharacterDataModified(const XMLString& prevValue, const XMLString& newValue);
	void setOwnerDocument(Document* pOwnerDocument);

	static const XMLString EMPTY_STRING;

private:
	AbstractNode();

	AbstractContainerNode* _pParent;
	AbstractNode*          _pNext;
	Document*              _pOwner;
	EventDispatcher*       _pEventDispatcher;

	static const XMLString NODE_NAME;

	friend class AbstractContainerNode;
	friend class Document;
	friend class DocumentFragment;
	friend class Element;
	friend class Attr;
	friend class CharacterData;
	friend class DOMBuilder;
	friend class NodeAppender;
};


} } // namespace Poco::XML


#endif // DOM_AbstractNode_INCLUDED
