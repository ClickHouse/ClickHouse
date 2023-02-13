//
// Comment.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM Comment class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_Comment_INCLUDED
#define DOM_Comment_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/CharacterData.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API Comment: public CharacterData
	/// This interface inherits from CharacterData and represents the content of
	/// a comment, i.e., all the characters between the starting '<!--' and ending
	/// '-->'. Note that this is the definition of a comment in XML, and, in practice,
	/// HTML, although some HTML tools may implement the full SGML comment structure.
{
public:
	// Node
	const XMLString& nodeName() const;
	unsigned short nodeType() const;

protected:
	Comment(Document* pOwnerDocument, const XMLString& data);
	Comment(Document* pOwnerDocument, const Comment& comment);
	~Comment();

	Node* copyNode(bool deep, Document* pOwnerDocument) const;

private:
	static const XMLString NODE_NAME;
	
	friend class Document;
};


} } // namespace Poco::XML


#endif // DOM_Comment_INCLUDED
