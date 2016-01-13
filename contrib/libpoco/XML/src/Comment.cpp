//
// Comment.cpp
//
// $Id: //poco/1.4/XML/src/Comment.cpp#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/Comment.h"


namespace Poco {
namespace XML {


const XMLString Comment::NODE_NAME = toXMLString("#comment");


Comment::Comment(Document* pOwnerDocument, const XMLString& data): 
	CharacterData(pOwnerDocument, data)
{
}


Comment::Comment(Document* pOwnerDocument, const Comment& comment): 
	CharacterData(pOwnerDocument, comment)
{
}


Comment::~Comment()
{
}


const XMLString& Comment::nodeName() const
{
	return NODE_NAME;
}


unsigned short Comment::nodeType() const
{
	return Node::COMMENT_NODE;
}


Node* Comment::copyNode(bool deep, Document* pOwnerDocument) const
{
	return new Comment(pOwnerDocument, *this);
}


} } // namespace Poco::XML
