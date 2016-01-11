//
// ChildNodesList.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/ChildNodesList.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the ChildNodesList class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_ChildNodesList_INCLUDED
#define DOM_ChildNodesList_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/NodeList.h"


namespace Poco {
namespace XML {


class XML_API ChildNodesList: public NodeList
	// This implementation of NodeList is returned
	// by Node::getChildNodes().
{
public:
	Node* item(unsigned long index) const;
	unsigned long length() const;

	void autoRelease();

protected:
	ChildNodesList(const Node* pParent);
	~ChildNodesList();
	
private:
	ChildNodesList();

	const Node* _pParent;
	
	friend class AbstractNode;
};


} } // namespace Poco::XML


#endif // DOM_ChildNodesList_INCLUDED
