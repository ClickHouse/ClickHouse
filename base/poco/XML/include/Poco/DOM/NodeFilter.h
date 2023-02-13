//
// NodeFilter.h
//
// Library: XML
// Package: DOM
// Module:  NodeFilter
//
// Definition of the DOM NodeFilter interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_NodeFilter_INCLUDED
#define DOM_NodeFilter_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class Node;


class XML_API NodeFilter
	/// Filters are objects that know how to "filter out" nodes. If a NodeIterator
	/// or TreeWalker is given a NodeFilter, it applies the filter before it returns
	/// the next node. If the filter says to accept the node, the traversal logic
	/// returns it; otherwise, traversal looks for the next node and pretends that
	/// the node that was rejected was not there.
	/// 
	/// The DOM does not provide any filters. NodeFilter is just an interface that
	/// users can implement to provide their own filters.
	/// 
	/// NodeFilters do not need to know how to traverse from node to node, nor do
	/// they need to know anything about the data structure that is being traversed.
	/// This makes it very easy to write filters, since the only thing they have
	/// to know how to do is evaluate a single node. One filter may be used with
	/// a number of different kinds of traversals, encouraging code reuse.
{
public:
	enum
	{
		FILTER_ACCEPT = 1,
			/// Accept the node. Navigation methods defined for NodeIterator or TreeWalker will return this node.

		FILTER_REJECT = 2,
			/// Reject the node. Navigation methods defined for NodeIterator or TreeWalker
			/// will not return this node. For TreeWalker, the children of this node will
			/// also be rejected. NodeIterators treat this as a synonym for FILTER_SKIP.

		FILTER_SKIP   = 3
			/// Skip this single node. Navigation methods defined for NodeIterator or TreeWalker
			/// will not return this node. For both NodeIterator and TreeWalker, the children
			/// of this node will still be considered.
	};
	
	enum WhatToShow
		/// These are the available values for the whatToShow parameter used in TreeWalkers
		/// and NodeIterators. They are the same as the set of possible types for Node,
		/// and their values are derived by using a bit position corresponding to the
		/// value of nodeType for the equivalent node type. If a bit in whatToShow is
		/// set false, that will be taken as a request to skip over this type of node;
		/// the behavior in that case is similar to that of FILTER_SKIP.
		/// 
		/// Note that if node types greater than 32 are ever introduced, they may not
		/// be individually testable via whatToShow. If that need should arise, it can
		/// be handled by selecting SHOW_ALL together with an appropriate NodeFilter.
	{
		SHOW_ALL                    = 0xFFFFFFFF,
			/// Show all Nodes.
	
		SHOW_ELEMENT                = 0x00000001,
			/// Show Element nodes.

		SHOW_ATTRIBUTE              = 0x00000002, 
			/// Show Attr nodes. This is meaningful only when creating an iterator or tree-walker
			/// with an attribute node as its root; in this case, it means that the attribute
			/// node will appear in the first position of the iteration or traversal. Since
			/// attributes are never children of other nodes, they do not appear when traversing
			/// over the document tree.

		SHOW_TEXT                   = 0x00000004,
			/// Show Text nodes.

		SHOW_CDATA_SECTION          = 0x00000008,
			/// Show CDATASection nodes.

		SHOW_ENTITY_REFERENCE       = 0x00000010,
			/// Show EntityReference nodes.

		SHOW_ENTITY                 = 0x00000020,
			/// Show Entity nodes. This is meaningful only when creating an iterator or
			/// tree-walker with an Entity node as its root; in this case, it means that
			/// the Entity node will appear in the first position of the traversal. Since
			/// entities are not part of the document tree, they do not appear when traversing
			/// over the document tree.

		SHOW_PROCESSING_INSTRUCTION = 0x00000040,
			/// Show ProcessingInstruction nodes.

		SHOW_COMMENT                = 0x00000080,
			/// Show Comment nodes.

		SHOW_DOCUMENT               = 0x00000100,
			/// Show Document nodes.

		SHOW_DOCUMENT_TYPE          = 0x00000200,
			/// Show DocumentType nodes.

		SHOW_DOCUMENT_FRAGMENT      = 0x00000400,
			/// Show DocumentFragment nodes.

		SHOW_NOTATION               = 0x00000800
			/// Show Notation nodes. This is meaningful only when creating an iterator or
			/// tree-walker with a Notation node as its root; in this case, it means that
			/// the Notation node will appear in the first position of the traversal. Since
			/// notations are not part of the document tree, they do not appear when traversing
			/// over the document tree.
	};
	
	virtual short acceptNode(Node* node) = 0;
		/// Test whether a specified node is visible in the logical view of a TreeWalker
		/// or NodeIterator. This function will be called by the implementation of TreeWalker
		/// and NodeIterator; it is not normally called directly from user code. (Though
		/// you could do so if you wanted to use the same filter to guide your own application
		/// logic.)
		///
		/// Returns FILTER_ACCEPT, FILTER_REJECT or FILTER_SKIP.
		
protected:
	virtual ~NodeFilter();
};


} } // namespace Poco::XML


#endif // DOM_NodeFilter_INCLUDED
