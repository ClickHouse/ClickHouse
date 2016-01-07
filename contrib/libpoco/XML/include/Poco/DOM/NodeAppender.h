//
// NodeAppender.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/NodeAppender.h#1 $
//
// Library: XML
// Package: DOM
// Module:  NodeAppender
//
// Definition of the NodeAppender class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_NodeAppender_INCLUDED
#define DOM_NodeAppender_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/Node.h"


namespace Poco {
namespace XML {


class AbstractNode;
class Element;


class XML_API NodeAppender
	/// The NodeAppender class provides a very fast way to
	/// build larger DOM documents.
	///
	/// In the DOM, child nodes are usually appended to a parent
	/// node using the appendChild() method. For nodes containing
	/// more than a few children, this method can be quite slow,
	/// due to the way it's implemented, and because of the 
	/// requirements of the DOM specification.
	///
	/// While the NodeAppender is being used on an Element, no
	/// children-modifying methods of that Element must be used.
	///
	/// This class is not part of the DOM specification.
{
public:
	NodeAppender(Element* parent);
		/// Creates the NodeAppender for the given parent node,
		/// which must be an Element.

	~NodeAppender();
		/// Destroys the NodeAppender.

	void appendChild(Node* newChild);
		/// Appends the node newChild to the end of the list of children of
		/// the parent node specified in the constructor.
		/// If the newChild is already in the tree, it is first removed.
		///
		/// NewChild can be a DocumentFragment. In this case, all children
		/// of the fragment become children of the parent element.
		///
		/// In order to speed up the function, no DOM events
		/// are fired.

private:
	NodeAppender();
	NodeAppender(const NodeAppender&);
	NodeAppender& operator = (const NodeAppender&);
	
	Element*      _pParent;
	AbstractNode* _pLast;
};


} } // namespace Poco::XML


#endif // #include "Poco/XML/XML.h"

