//
// Notation.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM Notation class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_Notation_INCLUDED
#define DOM_Notation_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractNode.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class XML_API Notation: public AbstractNode
	/// This interface represents a notation declared in the DTD. A notation either
	/// declares, by name, the format of an unparsed entity (see section 4.7 of
	/// the XML 1.0 specification <http://www.w3.org/TR/2004/REC-xml-20040204/>), 
	/// or is used for formal declaration of processing
	/// instruction targets (see section 2.6 of the XML 1.0 specification).
	/// The nodeName attribute inherited from Node is set to the declared name of
	/// the notation.
	/// 
	/// The DOM Level 1 does not support editing Notation nodes; they are therefore
	/// readonly.
	/// 
	/// A Notation node does not have any parent.
{
public:
	const XMLString& publicId() const;
		/// Returns the public identifier of this notation.
		/// If not specified, this is an empty string (and not null,
		/// as in the DOM specification).

	const XMLString& systemId() const;
		/// Returns the system identifier of this notation.
		/// If not specified, this is an empty string (and not null,
		/// as in the DOM specification).

	// Node
	const XMLString& nodeName() const;
	unsigned short nodeType() const;

protected:
	Notation(Document* pOwnerDocument, const XMLString& name, const XMLString& publicId, const XMLString& systemId);
	Notation(Document* pOwnerDocument, const Notation& notation);
	~Notation();
	
	Node* copyNode(bool deep, Document* pOwnerDocument) const;

private:
	XMLString _name;
	XMLString _publicId;
	XMLString _systemId;
	
	friend class Document;
};


//
// inlines
//
inline const XMLString& Notation::publicId() const
{
	return _publicId;
}


inline const XMLString& Notation::systemId() const
{
	return _systemId;
}


} } // namespace Poco::XML


#endif // DOM_Notation_INCLUDED
