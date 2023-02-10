//
// NodeList.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM NodeList interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_NodeList_INCLUDED
#define DOM_NodeList_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/DOMObject.h"


namespace Poco {
namespace XML {


class Node;


class XML_API NodeList: public DOMObject
	/// The NodeList interface provides the abstraction of an ordered
	/// collection of nodes, without defining or constraining how this
	/// collection is implemented.
	///
	/// The items in the NodeList are accessible via an integral index, 
	/// starting from 0.
	///
	/// A NodeList returned from a method must be released with a call to 
	/// release() when no longer needed.
{
public:
	virtual Node* item(unsigned long index) const = 0;
		/// Returns the index'th item in the collection. If index is
		/// greater than or equal to the number of nodes in the list,
		/// this returns null.

	virtual unsigned long length() const = 0;
		/// Returns the number of nodes in the list. The range of valid
		/// node indices is 0 to length - 1 inclusive.
	
protected:
	virtual ~NodeList();
};


} } // namespace Poco::XML


#endif // DOM_NodeList_INCLUDED
