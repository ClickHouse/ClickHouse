//
// DocumentType.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM DocumentType class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_DocumentType_INCLUDED
#define DOM_DocumentType_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/DOM/AbstractContainerNode.h"


namespace Poco {
namespace XML {


class NamedNodeMap;


class XML_API DocumentType: public AbstractContainerNode
	/// Each Document has a doctype attribute whose value is either null or a DocumentType
	/// object. The DocumentType interface in the DOM Level 1 Core provides an
	/// interface to the list of entities that are defined for the document, and
	/// little else because the effect of namespaces and the various XML scheme
	/// efforts on DTD representation are not clearly understood as of this writing.
	/// 
	/// The DOM Level 1 doesn't support editing DocumentType nodes.
{
public:
	const XMLString& name() const;
		/// The name of the DTD; i.e., the name immediately following the 
		/// DOCTYPE keyword.

	NamedNodeMap* entities() const;
		/// A NamedNodeMap containing the general entities,
		/// both external and internal, declared in the DTD.
		/// Duplicates are discarded.
		///
		/// Note: In this implementation, only the
		/// external entities are reported.
		/// Every node in this map also implements the
		/// Entity interface.
		/// 
		/// The returned NamedNodeMap must be released with a call
		/// to release() when no longer needed.

	NamedNodeMap* notations() const;
		/// A NamedNodeMap containing the notations declared in the DTD. Duplicates
		/// are discarded. Every node in this map also implements the Notation interface.
		/// The DOM Level 1 does not support editing notations, therefore notations
		/// cannot be altered in any way.
		/// 
		/// The returned NamedNodeMap must be released with a call
		/// to release() when no longer needed.

	// DOM Level 2
	const XMLString& publicId() const;
		/// Returns the public identifier of the external DTD subset.

	const XMLString& systemId() const;
		/// Returns the system identifier of the external DTD subset.

	const XMLString& internalSubset() const;
		/// Returns the internal DTD subset. This implementation
		/// returns an empty string.

	// Node
	const XMLString& nodeName() const;
	unsigned short nodeType() const;

protected:
	DocumentType(Document* pOwner, const XMLString& name, const XMLString& publicId, const XMLString& systemId);
	DocumentType(Document* pOwner, const DocumentType& dt);
	~DocumentType();

	Node* copyNode(bool deep, Document* pOwnerDocument) const;

private:
	XMLString _name;
	XMLString _publicId;
	XMLString _systemId;
	
	friend class DOMImplementation;
	friend class Document;
	friend class DOMBuilder;
};


//
// inlines
//
inline const XMLString& DocumentType::name() const
{
	return _name;
}


inline const XMLString& DocumentType::publicId() const
{
	return _publicId;
}


inline const XMLString& DocumentType::systemId() const
{
	return _systemId;
}


} } // namespace Poco::XML


#endif // DOM_DocumentType_INCLUDED
