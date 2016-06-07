//
// Element.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/Element.h#2 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM Element class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_Element_INCLUDED
#define DOM_Element_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractContainerNode.h"
#include "Poco/XML/Name.h"


namespace Poco {
namespace XML {


class Attr;
class NodeList;
class Document;


class XML_API Element: public AbstractContainerNode
	/// The Element interface represents an element in an XML document.
	/// Elements may have attributes associated with them; since the Element interface
	/// inherits from Node, the generic Node interface attribute attributes may
	/// be used to retrieve the set of all attributes for an element. There are
	/// methods on the Element interface to retrieve either an Attr object by name
	/// or an attribute value by name. In XML, where an attribute value may contain
	/// entity references, an Attr object should be retrieved to examine the possibly
	/// fairly complex sub-tree representing the attribute value. 
{
public:
	const XMLString& tagName() const;
		/// Returns the name of the element.
		///
		/// For example, in
		/// 
		///     <elementExample id="demo"> 
        ///         ... 
		///     </elementExample>
		///
		/// tagName has the value "elementExample". Note that this is case-preserving in XML, 
		/// as are all of the operations of the DOM.

	const XMLString& getAttribute(const XMLString& name) const;
		/// Retrieves an attribute value by name.
		///
		/// Returns the attribute's value, if the attribute
		/// exists, or an empty string otherwise.

	void setAttribute(const XMLString& name, const XMLString& value);
		/// Adds a new attribute. If an attribute with that name is already present
		/// in the element, its value is changed to be that of the value parameter.
		/// This value is a simple string; it is not parsed as it is being set. So any
		/// markup (such as syntax to be recognized as an entity reference) is treated
		/// as literal text, and needs to be appropriately escaped by the implementation
		/// when it is written out.
 
	void removeAttribute(const XMLString& name);
		/// Removes an attribute by name.

	Attr* getAttributeNode(const XMLString& name) const;
		/// Retrieves an Attr node by name.

	Attr* setAttributeNode(Attr* newAttr);
		/// Adds a new attribute. If an attribute with that name is already
		/// present in the element, it is replaced by the new one.

	Attr* addAttributeNodeNP(Attr* oldAttr, Attr* newAttr);
		/// For internal use only.
		/// Adds a new attribute after oldAttr.
		/// If oldAttr is 0, newAttr is set as first attribute.
		/// Returns newAttr.
		/// Does not fire any events.

	Attr* removeAttributeNode(Attr* oldAttr);
		/// Removes the specified attribute.

	NodeList* getElementsByTagName(const XMLString& name) const;
		/// Returns a NodeList of all descendant elements with a given tag
		/// name, in the order in which they would be encountered in a
		/// preorder traversal of the Element tree.
		///
		/// The special name "*" matches all tags.
		///
		/// The returned NodeList must be released with a call
		/// to release() when no longer needed.

	void normalize();
		/// Puts all Text nodes in the full depth of the sub-tree underneath this Element,
		/// including attribute nodes, into a "normal" form where only markup (e.g.,
		/// tags, comments, processing instructions, CDATA sections, and entity references)
		/// separates Text nodes, i.e., there are no adjacent Text nodes. This can be
		/// used to ensure that the DOM view of a document is the same as if it were
		/// saved and re-loaded, and is useful when operations (such as XPointer
		/// lookups) that depend on a particular document tree structure are to be used.
		/// 
		/// Note: In cases where the document contains CDATASections, the normalize
		/// operation alone may not be sufficient, since XPointers do not differentiate
		/// between Text nodes and CDATASection nodes.

	// DOM Level 2
	const XMLString& getAttributeNS(const XMLString& namespaceURI, const XMLString& localName) const;
		/// Retrieves an attribute value by name.
		///
		/// Returns the attribute's value, if the attribute
		/// exists, or an empty string otherwise.

	void setAttributeNS(const XMLString& namespaceURI, const XMLString& qualifiedName, const XMLString& value);
		/// Adds a new attribute. If an attribute with that name
		/// is already present in the element, its value is changed
		/// to be that of the value parameter.

	void removeAttributeNS(const XMLString& namespaceURI, const XMLString& localName);
		/// Removes an attribute by name.

	Attr* getAttributeNodeNS(const XMLString& namespaceURI, const XMLString& localName) const;
		/// Retrieves an Attr node by name.

	Attr* setAttributeNodeNS(Attr* newAttr);
		/// Adds a new attribute. If an attribute with that name is already
		/// present in the element, it is replaced by the new one.

	bool hasAttribute(const XMLString& name) const;
		/// Returns true if and only if the element has the specified attribute.

	bool hasAttributeNS(const XMLString& namespaceURI, const XMLString& localName) const;
		/// Returns true if and only if the element has the specified attribute.

	NodeList* getElementsByTagNameNS(const XMLString& namespaceURI, const XMLString& localName) const;
		/// Returns a NodeList of all the descendant Elements with a given local name and namespace URI 
		/// in the order in which they are encountered in a preorder traversal of this Element tree.
		///
		/// The special value "*" matches all namespaces, or local names respectively.
		///
		/// The returned NodeList must be released with a call
		/// to release() when no longer needed.

	const XMLString& namespaceURI() const;
	XMLString prefix() const;
	const XMLString& localName() const;
	bool hasAttributes() const;
	XMLString innerText() const;

	Element* getChildElement(const XMLString& name) const;
		/// Returns the first child element with the given name, or null
		/// if such an element does not exist.
		///
		/// This method is an extension to the W3C Document Object Model.

	Element* getChildElementNS(const XMLString& namespaceURI, const XMLString& localName) const;
		/// Returns the first child element with the given namespaceURI and localName,
		/// or null if such an element does not exist.
		///
		/// This method is an extension to the W3C Document Object Model.
	
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
	
	// Node
	const XMLString& nodeName() const;
	NamedNodeMap* attributes() const;
	unsigned short nodeType() const;

protected:
	Element(Document* pOwnerDocument, const XMLString& namespaceURI, const XMLString& localName, const XMLString& qname);
	Element(Document* pOwnerDocument, const Element& elem);
	~Element();

	Node* copyNode(bool deep, Document* pOwnerDocument) const;

	void dispatchNodeRemovedFromDocument();
	void dispatchNodeInsertedIntoDocument();

private:
	const Name& _name;
	Attr*       _pFirstAttr;

	friend class Attr;
	friend class Document;
	friend class AttrMap;
};


//
// inlines
//
inline const XMLString& Element::tagName() const
{
	return _name.qname();
}


} } // namespace Poco::XML


#endif // DOM_Element_INCLUDED
