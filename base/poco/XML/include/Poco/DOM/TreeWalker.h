//
// TreeWalker.h
//
// Library: XML
// Package: DOM
// Module:  TreeWalker
//
// Definition of the DOM TreeWalker class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_TreeWalker_INCLUDED
#define DOM_TreeWalker_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class Node;
class NodeFilter;


class XML_API TreeWalker
	/// TreeWalker objects are used to navigate a document tree or subtree using
	/// the view of the document defined by their whatToShow flags and filter (if
	/// any). Any function which performs navigation using a TreeWalker will automatically
	/// support any view defined by a TreeWalker.
	/// 
	/// Omitting nodes from the logical view of a subtree can result in a structure
	/// that is substantially different from the same subtree in the complete, unfiltered
	/// document. Nodes that are siblings in the TreeWalker view may be children
	/// of different, widely separated nodes in the original view. For instance,
	/// consider a NodeFilter that skips all nodes except for Text nodes and the
	/// root node of a document. In the logical view that results, all text nodes
	/// will be siblings and appear as direct children of the root node, no matter
	/// how deeply nested the structure of the original document.
	///
	/// A TreeWalker can be directly instantiated using one of its constructors -
	/// the DocumentTraversal interface is not needed and therefore not implemented.
	/// Unlike most other DOM classes, TreeWalker supports value semantics.
	///
	/// If the TreeWalker's current node is removed from the document, the
	/// result of calling any of the movement methods is undefined. This behavior 
	/// does not conform to the DOM Level 2 Traversal specification.
{
public:
	TreeWalker(Node* root, unsigned long whatToShow, NodeFilter* pFilter = 0);
		/// Creates a TreeWalker over the subtree rooted at the specified node.
		
	TreeWalker(const TreeWalker& walker);
		/// Creates a TreeWalker by copying another TreeWalker.
		
	TreeWalker& operator = (const TreeWalker& walker);
		/// Assignment operator.
	
	~TreeWalker();
		/// Destroys the TreeWalker.
	
	Node* root() const;
		/// The root node of the TreeWalker, as specified when it was created.

	unsigned long whatToShow() const;
		/// This attribute determines which node types are presented via the TreeWalker.
		/// The available set of constants is defined in the NodeFilter interface. Nodes
		/// not accepted by whatToShow will be skipped, but their children may still
		/// be considered. Note that this skip takes precedence over the filter, if
		/// any.

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

	Node* currentNode() const;
		/// The node at which the TreeWalker is currently positioned.
		/// Alterations to the DOM tree may cause the current node to no longer be accepted
		/// by the TreeWalker's associated filter. currentNode may also be explicitly
		/// set to any node, whether or not it is within the subtree specified by the
		/// root node or would be accepted by the filter and whatToShow flags. Further
		/// traversal occurs relative to currentNode even if it is not part of the current
		/// view, by applying the filters in the requested direction; if no traversal
		/// is possible, currentNode is not changed.

	Node* getCurrentNode() const;
		/// See currentNode().
		
	void setCurrentNode(Node* pNode);
		/// Sets the current node.

	Node* parentNode();
		/// Moves to and returns the closest visible ancestor node of the current node.
		/// If the search for parentNode attempts to step upward from the TreeWalker's
		/// root node, or if it fails to find a visible ancestor node, this method retains
		/// the current position and returns null.

	Node* firstChild();
		/// Moves the TreeWalker to the first visible child of the current node, and
		/// returns the new node. If the current node has no visible children, returns
		/// null, and retains the current node.

	Node* lastChild();
		/// Moves the TreeWalker to the last visible child of the current node, and
		/// returns the new node. If the current node has no visible children, returns
		/// null, and retains the current node.

	Node* previousSibling();
		/// Moves the TreeWalker to the previous sibling of the current node, and returns
		/// the new node. If the current node has no visible previous sibling, returns
		/// null, and retains the current node.

	Node* nextSibling();
		/// Moves the TreeWalker to the next sibling of the current node, and returns
		/// the new node. If the current node has no visible next sibling, returns null,
		/// and retains the current node.

	Node* previousNode();
		/// Moves the TreeWalker to the previous visible node in document order relative
		/// to the current node, and returns the new node. If the current node has no
		/// previous node, or if the search for previousNode attempts to step upward
		/// from the TreeWalker's root node, returns null, and retains the current node.

	Node* nextNode();
		/// Moves the TreeWalker to the next visible node in document order relative
		/// to the current node, and returns the new node. If the current node has no
		/// next node, or if the search for nextNode attempts to step upward from the
		/// TreeWalker's root node, returns null, and retains the current node.

protected:
	int accept(Node* pNode) const;
	Node* next(Node* pNode) const;
	Node* previous(Node* pNode) const;

private:
	TreeWalker();
	
	Node*         _pRoot;
	unsigned long _whatToShow;
	NodeFilter*   _pFilter;
	Node*         _pCurrent;
};


//
// inlines
//
inline Node* TreeWalker::root() const
{
	return _pRoot;
}


inline unsigned long TreeWalker::whatToShow() const
{
	return _whatToShow;
}


inline NodeFilter* TreeWalker::filter() const
{
	return _pFilter;
}


inline bool TreeWalker::expandEntityReferences() const
{
	return false;
}


inline Node* TreeWalker::currentNode() const
{
	return _pCurrent;
}


inline Node* TreeWalker::getCurrentNode() const
{
	return _pCurrent;
}


} } // namespace Poco::XML


#endif // DOM_TreeWalker_INCLUDED
