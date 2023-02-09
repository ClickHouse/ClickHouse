//
// NodeIterator.h
//
// Library: XML
// Package: DOM
// Module:  NodeIterator
//
// Definition of the DOM NodeIterator class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_NodeIterator_INCLUDED
#define DOM_NodeIterator_INCLUDED


#include "Poco/XML/XML.h"


namespace Poco {
namespace XML {


class Node;
class NodeFilter;


class XML_API NodeIterator
	/// Iterators are used to step through a set of nodes, e.g. the set of nodes
	/// in a NodeList, the document subtree governed by a particular Node, the results
	/// of a query, or any other set of nodes. The set of nodes to be iterated is
	/// determined by the implementation of the NodeIterator. DOM Level 2 specifies
	/// a single NodeIterator implementation for document-order traversal of a document
	/// subtree.
	///
	/// A NodeIterator can be directly instantiated using one of its constructors -
	/// the DocumentTraversal interface is not needed and therefore not implemented.
	/// Unlike most other DOM classes, NodeIterator supports value semantics.
	///
	/// If the NodeIterator's current node is removed from the document, the
	/// result of calling any of the movement methods is undefined. This behavior does
	/// not conform to the DOM Level 2 Traversal specification.
{
public:
	NodeIterator(Node* root, unsigned long whatToShow, NodeFilter* pFilter = 0);
		/// Creates a NodeIterator over the subtree rooted at the specified node.
		
	NodeIterator(const NodeIterator& iterator);
		/// Creates a NodeIterator by copying another NodeIterator.
		
	NodeIterator& operator = (const NodeIterator& iterator);
		/// Assignment operator.
		
	~NodeIterator();
		/// Destroys the NodeIterator.

	Node* root() const;
		/// The root node of the NodeIterator, as specified when it was created.

	unsigned long whatToShow() const;
		/// This attribute determines which node types are presented via the iterator. 
		/// The available set of constants is defined in the NodeFilter interface. 
		/// Nodes not accepted by whatToShow will be skipped, but their children may 
		/// still be considered. Note that this skip takes precedence over the filter, 
		/// if any.

	NodeFilter* filter() const;
		/// The NodeFilter used to screen nodes.

	bool expandEntityReferences() const;
		/// The value of this flag determines whether the children of entity reference
		/// nodes are visible to the iterator. If false, they and their descendants
		/// will be rejected. Note that this rejection takes precedence over whatToShow
		/// and the filter. Also note that this is currently the only situation where
		/// NodeIterators may reject a complete subtree rather than skipping individual
		/// nodes.
		/// 
		/// To produce a view of the document that has entity references expanded and
		/// does not expose the entity reference node itself, use the whatToShow flags
		/// to hide the entity reference node and set expandEntityReferences to true
		/// when creating the iterator. To produce a view of the document that has entity
		/// reference nodes but no entity expansion, use the whatToShow flags to show
		/// the entity reference node and set expandEntityReferences to false.
		///
		/// This implementation does not support entity reference expansion and
		/// thus always returns false.

	Node* nextNode();
		/// Returns the next node in the set and advances the position of the iterator
		/// in the set. After a NodeIterator is created, the first call to nextNode()
		/// returns the first node in the set.

	Node* previousNode();
		/// Returns the previous node in the set and moves the position of the NodeIterator
		/// backwards in the set.

	Node* currentNodeNP() const;
		/// Returns the current node in the set.
		///
		/// Leaves the NodeIterator unchanged.
		///
		/// Warning: This is a proprietary extension to the DOM Level 2 NodeIterator
		/// interface.

	void detach();
		/// Detaches the NodeIterator from the set which it iterated over, releasing
		/// any computational resources and placing the iterator in the INVALID state.
		/// After detach has been invoked, calls to nextNode or previousNode will raise
		/// the exception INVALID_STATE_ERR.

protected:
	bool accept(Node* pNode) const;
	Node* next() const;
	Node* previous() const;
	Node* last();

private:
	NodeIterator();
	
	Node*         _pRoot;
	unsigned long _whatToShow;
	NodeFilter*   _pFilter;
	Node*         _pCurrent;
};


//
// inlines
//
inline Node* NodeIterator::root() const
{
	return _pRoot;
}


inline Node* NodeIterator::currentNodeNP() const
{
	return _pCurrent;
}


inline unsigned long NodeIterator::whatToShow() const
{
	return _whatToShow;
}


inline NodeFilter* NodeIterator::filter() const
{
	return _pFilter;
}


inline bool NodeIterator::expandEntityReferences() const
{
	return false;
}


} } // namespace Poco::XML


#endif // DOM_NodeIterator_INCLUDED
