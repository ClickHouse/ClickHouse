//
// Document.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/Document.h#2 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM Document class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_Document_INCLUDED
#define DOM_Document_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractContainerNode.h"
#include "Poco/DOM/DocumentEvent.h"
#include "Poco/DOM/Element.h"
#include "Poco/XML/XMLString.h"
#include "Poco/XML/NamePool.h"
#include "Poco/AutoReleasePool.h"


namespace Poco {
namespace XML {


class NamePool;
class DocumentType;
class DOMImplementation;
class DocumentFragment;
class Text;
class Comment;
class CDATASection;
class ProcessingInstruction;
class Attr;
class EntityReference;
class NodeList;
class Entity;
class Notation;


class XML_API Document: public AbstractContainerNode, public DocumentEvent
	/// The Document interface represents the entire HTML or XML document. Conceptually, 
	/// it is the root of the document tree, and provides the primary access to the 
	/// document's data.
	///
	/// Since elements, text nodes, comments, processing instructions, etc. cannot exist 
	/// outside the context of a Document, the Document interface also contains the 
	/// factory methods needed to create these objects. The Node objects created have a 
	/// ownerDocument attribute which associates them with the Document within whose 
	/// context they were created.
{
public:
	typedef Poco::AutoReleasePool<DOMObject> AutoReleasePool;

	Document(NamePool* pNamePool = 0);
		/// Creates a new document. If pNamePool == 0, the document
		/// creates its own name pool, otherwise it uses the given name pool.
		/// Sharing a name pool makes sense for documents containing instances
		/// of the same schema, thus reducing memory usage.

	Document(DocumentType* pDocumentType, NamePool* pNamePool = 0);
		/// Creates a new document. If pNamePool == 0, the document
		/// creates its own name pool, otherwise it uses the given name pool.
		/// Sharing a name pool makes sense for documents containing instances
		/// of the same schema, thus reducing memory usage.

	NamePool& namePool();
		/// Returns a pointer to the documents Name Pool.

	AutoReleasePool& autoReleasePool();
		/// Returns a pointer to the documents Auto Release Pool.

	void collectGarbage();
		/// Releases all objects in the Auto Release Pool.

	void suspendEvents();
		/// Suspends all events until resumeEvents() is called.

	void resumeEvents();
		/// Resumes all events suspended with suspendEvent();

	bool eventsSuspended() const;
		/// Returns true if events are suspeded.

	bool events() const;
		/// Returns true if events are not suspeded.

	const DocumentType* doctype() const;
		/// The Document Type Declaration (see DocumentType) associated with this document.
		/// For HTML documents as well as XML documents without a document type declaration
		/// this returns null. The DOM Level 1 does not support editing the Document
		/// Type Declaration. docType cannot be altered in any way, including through
		/// the use of methods inherited from the Node interface, such as insertNode
		/// or removeNode.

	const DOMImplementation& implementation() const;
		/// The DOMImplementation object that handles this document. A DOM application
		/// may use objects from multiple implementations.

	Element* documentElement() const;
		/// This is a convenience attribute that allows direct access to the child node
		/// that is the root element of the document. For HTML documents, this is the
		/// element with the tagName "HTML".

	Element* createElement(const XMLString& tagName) const;
		/// Creates an element of the type specified. Note that the instance returned
		/// implements the Element interface, so attributes can be specified directly
		/// on the returned object.
		///
		/// In addition, if there are known attributes with default values, Attr nodes
		/// representing them are automatically created and attached to the element.

	DocumentFragment* createDocumentFragment() const;
		/// Creates an empty DocumentFragment object.

	Text* createTextNode(const XMLString& data) const;
		/// Creates a text node given the specified string.

	Comment* createComment(const XMLString& data) const;
		/// Creates a comment node given the specified string.

	CDATASection* createCDATASection(const XMLString& data) const;
		/// Creates a CDATASection node whose value is the specified string.

	ProcessingInstruction* createProcessingInstruction(const XMLString& target, const XMLString& data) const;
		/// Creates a ProcessingInstruction node given the specified target and data strings.

	Attr* createAttribute(const XMLString& name) const;	
		/// Creates an Attr of the given name. Note that the Attr instance can then
		/// be set on an Element using the setAttributeNode method.	

	EntityReference* createEntityReference(const XMLString& name) const;
		/// Creates an EntityReference object. In addition, if the referenced entity
		/// is known, the child list of the EntityReference node is made the same as
		/// that of the corresponding Entity node.

	NodeList* getElementsByTagName(const XMLString& name) const;
		/// Returns a NodeList of all Elements with a given tag name in the order
		/// in which they would be encountered in a preorder traversal of the
		/// document tree.
		///
		/// The returned NodeList must be released with a call to release()
		/// when no longer needed.

	// DOM Level 2
	Node* importNode(Node* importedNode, bool deep);
		/// Imports a node from another document to this document. The returned node
		/// has no parent; (parentNode is null). The source node is not altered or removed
		/// from the original document; this method creates a new copy of the source
		/// node.
		/// For all nodes, importing a node creates a node object owned by the importing
		/// document, with attribute values identical to the source node's nodeName
		/// and nodeType, plus the attributes related to namespaces (prefix, localName,
		/// and namespaceURI). As in the cloneNode operation on a Node, the source node
		/// is not altered.
		/// Additional information is copied as appropriate to the nodeType, attempting
		/// to mirror the behavior expected if a fragment of XML or HTML source was
		/// copied from one document to another, recognizing that the two documents
		/// may have different DTDs in the XML case.

	Element* createElementNS(const XMLString& namespaceURI, const XMLString& qualifiedName) const;
		/// Creates an element of the given qualified name and namespace URI.

	Attr* createAttributeNS(const XMLString& namespaceURI, const XMLString& qualifiedName) const;
		/// Creates an attribute of the given qualified name and namespace URI.

	NodeList* getElementsByTagNameNS(const XMLString& namespaceURI, const XMLString& localName) const;
		/// Returns a NodeList of all the Elements with a given local name and 
		/// namespace URI in the order in which they are encountered in a 
		/// preorder traversal of the Document tree.

	Element* getElementById(const XMLString& elementId) const;
		/// Returns the Element whose ID is given by elementId. If no such 
		/// element exists, returns null. Behavior is not defined if more
		/// than one element has this ID. 
		///
		/// Note: The DOM implementation must have information that says 
		/// which attributes are of type ID. Attributes with the name "ID"
		/// are not of type ID unless so defined. Implementations that do 
		/// not know whether attributes are of type ID or not are expected to
		/// return null. This implementation therefore returns null.
		///
		/// See also the non-standard two argument variant of getElementById()
		/// and getElementByIdNS().

	// DocumentEvent
	Event* createEvent(const XMLString& eventType) const;

	// Node
	const XMLString& nodeName() const;
	unsigned short nodeType() const;

	// EventTarget
	bool dispatchEvent(Event* evt);
	
	// Extensions
	Entity* createEntity(const XMLString& name, const XMLString& publicId, const XMLString& systemId, const XMLString& notationName) const;
		/// Creates an Entity with the given name, publicId, systemId and notationName.
		///
		/// This method is not part of the W3C Document Object Model.

	Notation* createNotation(const XMLString& name, const XMLString& publicId, const XMLString& systemId) const;
		/// Creates a Notation with the given name, publicId and systemId.
		///
		/// This method is not part of the W3C Document Object Model.

	Element* getElementById(const XMLString& elementId, const XMLString& idAttribute) const;
		/// Returns the first Element whose ID attribute (given in idAttribute)
		/// has the given elementId. If no such element exists, returns null. 
		///
		/// This method is an extension to the W3C Document Object Model.

	Element* getElementByIdNS(const XMLString& elementId, const XMLString& idAttributeURI, const XMLString& idAttributeLocalName) const;
		/// Returns the first Element whose ID attribute (given in idAttributeURI and idAttributeLocalName)
		/// has the given elementId. If no such element exists, returns null. 
		///
		/// This method is an extension to the W3C Document Object Model.

protected:
	~Document();

	Node* copyNode(bool deep, Document* pOwnerDocument) const;
	
	DocumentType* getDoctype();
	void setDoctype(DocumentType* pDoctype);

private:
	DocumentType*   _pDocumentType;
	NamePool*       _pNamePool;
	AutoReleasePool _autoReleasePool;
	int             _eventSuspendLevel;

	static const XMLString NODE_NAME;
	
	friend class DOMBuilder;
};


//
// inlines
//
inline NamePool& Document::namePool()
{
	return *_pNamePool;
}


inline Document::AutoReleasePool& Document::autoReleasePool()
{
	return _autoReleasePool;
}


inline const DocumentType* Document::doctype() const
{
	return _pDocumentType;
}


inline DocumentType* Document::getDoctype()
{
	return _pDocumentType;
}


} } // namespace Poco::XML


#endif // DOM_Document_INCLUDED
