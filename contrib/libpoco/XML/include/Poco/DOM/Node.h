//
// Node.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/Node.h#2 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM Node interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_Node_INCLUDED
#define DOM_Node_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/EventTarget.h"
#include "Poco/XML/XMLString.h"
#include "Poco/SAX/NamespaceSupport.h"


namespace Poco {
namespace XML {


class NamedNodeMap;
class Document;
class NodeList;


class XML_API Node: public EventTarget
	/// The Node interface is the primary datatype for the entire Document Object
	/// Model. It represents a single node in the document tree. While all objects
	/// implementing the Node interface expose methods for dealing with children,
	/// not all objects implementing the Node interface may have children. For
	/// example, Text nodes may not have children, and adding children to such
	/// nodes results in a DOMException being raised.
	/// 
	/// The attributes nodeName, nodeValue and attributes are included as a mechanism
	/// to get at node information without casting down to the specific derived
	/// interface. In cases where there is no obvious mapping of these attributes
	/// for a specific nodeType (e.g., nodeValue for an Element or attributes for
	/// a Comment), this returns null. Note that the specialized interfaces may
	/// contain additional and more convenient mechanisms to get and set the relevant
	/// information.
	///
	/// This implementation differs in some ways from the W3C DOM recommendations.
	/// For example, the DOM specifies that some methods can return null strings.
	/// Instead of null strings, this implementation always returns empty strings.
{
public:
	enum
	{
		ELEMENT_NODE = 1,             /// The node is an Element.
		ATTRIBUTE_NODE,               /// The node is an Attr.
		TEXT_NODE,                    /// The node is a Text node.
		CDATA_SECTION_NODE,           /// The node is a CDATASection.
		ENTITY_REFERENCE_NODE,        /// The node is an EntityReference.
		ENTITY_NODE,                  /// The node is an Entity.
		PROCESSING_INSTRUCTION_NODE,  /// The node is a ProcessingInstruction.
		COMMENT_NODE,                 /// The node is a Comment.
		DOCUMENT_NODE,                /// The node is a Document.
		DOCUMENT_TYPE_NODE,           /// The node is a DocumentType.
		DOCUMENT_FRAGMENT_NODE,       /// The node is a DocumentFragment.
		NOTATION_NODE                 /// The node is a Notation.
	};

	virtual const XMLString& nodeName() const = 0;
		/// Returns the name of this node, depending on its type.

	const XMLString& nodeValue() const;
		/// Returns the value of this node, depending on its type.

	virtual const XMLString& getNodeValue() const = 0;
		/// Returns the value of this node, depending on its type.

	virtual void setNodeValue(const XMLString& value) = 0;
		/// Sets the value of this node. Throws an exception
		/// if the node is read-only.

	virtual unsigned short nodeType() const = 0;
		/// Returns a code representing the type of the underlying object.

	virtual Node* parentNode() const = 0;
		/// The parent of this node. All nodes, except Attr, Document, DocumentFragment,
		/// Entity, and Notation may have a parent. However, if a node has just been
		/// created and not yet added to the tree, or if it has been removed from the
		/// tree, this is null.

	virtual NodeList* childNodes() const = 0;
		/// Returns a NodeList containing all children of this node.
		///
		/// The returned NodeList must be released with a call
		/// to release() when no longer needed.

	virtual Node* firstChild() const = 0;
		/// Returns the first child of this node. If there is no such
		/// node, this returns null.

	virtual Node* lastChild() const = 0;
		/// Returns the last child of this node. If there is no such
		/// node, this returns null.

	virtual Node* previousSibling() const = 0;
		/// Returns the node immediately preceding this node. If there
		/// is no such node, this returns null.

	virtual Node* nextSibling() const = 0;
		/// Returns the node immediately following this node. If there
		/// is no such node, this returns null.

	virtual NamedNodeMap* attributes() const = 0;
		/// Returns a NamedNodeMap containing the attributes of this
		/// node (if it is an Element) or null otherwise.
		/// 
		/// The returned NamedNodeMap must be released with a call
		/// to release() when no longer needed.

	virtual Document* ownerDocument() const = 0;
		/// Returns the Document object associated with this node.
		/// This is also the Document object used to create new nodes.
		/// When this node is a Document, this is null.

	virtual Node* insertBefore(Node* newChild, Node* refChild) = 0;
		/// Inserts the node newChild before the existing child node refChild.
		///
		/// If refChild is null, insert newChild at the end of the list of children.
		/// If newChild is a DocumentFragment object, all of its children are
		/// inserted in the same order, before refChild. If the newChild is already
		/// in the tree, it is first removed.

	virtual Node* replaceChild(Node* newChild, Node* oldChild) = 0;
		/// Replaces the child node oldChild with newChild in the list of children,
		/// and returns the oldChild node.
		/// If newChild is a DocumentFragment object, oldChild is replaced by all of
		/// the DocumentFragment children, which are inserted in the same order. If
		/// the newChild is already in the tree, it is first removed.

	virtual Node* removeChild(Node* oldChild) = 0;
		/// Removes the child node indicated by oldChild from the list of children
		/// and returns it. 

	virtual Node* appendChild(Node* newChild) = 0;
		/// Appends the node newChild to the end of the list of children of this node.
		/// If newChild is already in the tree, it is first removed.

	virtual bool hasChildNodes() const = 0;
		/// This is a convenience method to allow easy determination of whether a 
		/// node has any children.
		/// Returns true if the node has any children, false otherwise.

	virtual Node* cloneNode(bool deep) const = 0;
		/// Returns a duplicate of this node, i.e., serves as a generic copy constructor
		/// for nodes. The duplicate node has no parent; (parentNode is null.).
		/// Cloning an Element copies all attributes and their values, including those
		/// generated by the XML processor to represent defaulted attributes, but this
		/// method does not copy any text it contains unless it is a deep clone, since
		/// the text is contained in a child Text node. Cloning an Attribute directly,
		/// as opposed to be cloned as part of an Element cloning operation, returns
		/// a specified attribute (specified is true). Cloning any other type of node
		/// simply returns a copy of this node.
		/// Note that cloning an immutable subtree results in a mutable copy, but the
		/// children of an EntityReference clone are readonly. In addition, clones of
		/// unspecified Attr nodes are specified. And, cloning Document, DocumentType,
		/// Entity, and Notation nodes is implementation dependent.
	
	// DOM Level 2
	virtual void normalize() = 0;
		/// Puts all Text nodes in the full depth of the sub-tree underneath this Node,
		/// including attribute nodes, into a "normal" form where only structure (e.g.,
		/// elements, comments, processing instructions, CDATA sections, and entity
		/// references) separates Text nodes, i.e., there are neither adjacent Text
		/// nodes nor empty Text nodes. This can be used to ensure that the DOM view
		/// of a document is the same as if it were saved and re-loaded, and is useful
		/// when operations (such as XPointer lookups) that depend on a particular
		/// document tree structure are to be used.
		/// 
		/// Note: In cases where the document contains CDATASections, the normalize
		/// operation alone may not be sufficient, since XPointers do not differentiate
		/// between Text nodes and CDATASection nodes.

	virtual bool isSupported(const XMLString& feature, const XMLString& version) const = 0;
		/// Tests whether the DOM implementation implements a specific 
		/// feature and that feature is supported by this node.

	virtual const XMLString& namespaceURI() const = 0;
		/// Returns the namespace URI of the node.
		/// This is not a computed value that is the result of a namespace lookup based on an 
		/// examination of the namespace declarations in scope. It is merely the namespace URI 
		/// given at creation time.
		///
		/// For nodes of any type other than ELEMENT_NODE and ATTRIBUTE_NODE and nodes created with a 
		/// DOM Level 1 method, such as createElement from the Document interface, this is always the
		/// empty string.

	virtual XMLString prefix() const = 0;
		/// Returns the namespace prefix from the qualified name of the node.

	virtual const XMLString& localName() const = 0;
		/// Returns the local name of the node.

	virtual bool hasAttributes() const = 0;
		/// Returns whether this node (if it is an element) has any attributes.
		
	// Extensions
	typedef Poco::XML::NamespaceSupport NSMap;

	virtual XMLString innerText() const = 0;
		/// Returns a string containing the concatenated values of the node
		/// and all its child nodes. 
		///
		/// This method is not part of the W3C Document Object Model.
		
	virtual Node* getNodeByPath(const XMLString& path) const = 0;
		/// Searches a node (element or attribute) based on a simplified XPath 
		/// expression.
		///
		/// Only simple XPath expressions are supported. These are the slash
		/// notation for specifying paths to elements, and the square bracket
		/// expression for finding elements by their index, by attribute value, 
		/// or finding attributes by names.
		///
		/// The slash at the beginning is optional, the evaluation always starts
		/// at this element. A double-slash at the beginning recursively searches 
		/// the entire subtree for the first element.
		///
		/// Examples:
		///     elem1/elem2/elem3
		///     /elem1/elem2/elem3
		///     /elem1/elem2[1]
		///     /elem1/elem2[@attr1]
		///     /elem1/elem2[@attr1='value']
		///     //elem2[@attr1='value']
		///     //[@attr1='value']
		///
		/// This method is an extension to the W3C Document Object Model.

	virtual Node* getNodeByPathNS(const XMLString& path, const NSMap& nsMap) const = 0;
		/// Searches a node (element or attribute) based on a simplified XPath 
		/// expression. The given NSMap must contain mappings from namespace
		/// prefixes to namespace URIs for all namespace prefixes used in 
		/// the path expression.
		///
		/// Only simple XPath expressions are supported. These are the slash
		/// notation for specifying paths to elements, and the square bracket
		/// expression for finding elements by their index, by attribute value, 
		/// or finding attributes by names.
		///
		/// The slash at the beginning is optional, the evaluation always starts
		/// at this element. A double-slash at the beginning recursively searches 
		/// the entire subtree for the first element.
		///
		/// Examples:
		///     /ns1:elem1/ns2:elem2/ns2:elem3
		///     /ns1:elem1/ns2:elem2[1]
		///     /ns1:elem1/ns2:elem2[@attr1]
		///     /ns1:elem1/ns2:elem2[@attr1='value']
		///     //ns2:elem2[@ns1:attr1='value']
		///     //[@ns1:attr1='value']
		///
		/// This method is an extension to the W3C Document Object Model.

protected:
	virtual ~Node();
};


//
// inlines
//
inline const XMLString& Node::nodeValue() const
{
	return getNodeValue();
}


} } // namespace Poco::XML


#endif // DOM_Node_INCLUDED
