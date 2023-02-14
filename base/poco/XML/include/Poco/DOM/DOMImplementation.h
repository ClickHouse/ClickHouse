//
// DOMImplementation.h
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOM DOMImplementation class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_DOMImplementation_INCLUDED
#define DOM_DOMImplementation_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class DocumentType;
class Document;
class NamePool;


class XML_API DOMImplementation
	/// The DOMImplementation interface provides a number of methods for
	/// performing operations that are independent of any particular instance
	/// of the document object model.
	/// In this implementation, DOMImplementation is implemented as a singleton.
{
public:
	DOMImplementation();
		/// Creates the DOMImplementation.
		
	~DOMImplementation();
		/// Destroys the DOMImplementation.

	bool hasFeature(const XMLString& feature, const XMLString& version) const;
		/// Tests if the DOM implementation implements a specific feature.
		///
		/// The only supported features are "XML", version "1.0" and "Core", 
		/// "Events", "MutationEvents" and "Traversal", version "2.0".
	
	// DOM Level 2
	DocumentType* createDocumentType(const XMLString& name, const XMLString& publicId, const XMLString& systemId) const;
		/// Creates an empty DocumentType node. Entity declarations and notations 
		/// are not made available. Entity reference expansions and default attribute 
		/// additions do not occur.

	Document* createDocument(const XMLString& namespaceURI, const XMLString& qualifiedName, DocumentType* doctype) const;
		/// Creates an XML Document object of the specified type with its document element.
		///
		/// Note: You can also create a Document directly using the new operator.

	static const DOMImplementation& instance();
		/// Returns a reference to the default DOMImplementation
		/// object.
		
private:
	static const XMLString FEATURE_XML;
	static const XMLString FEATURE_CORE;
	static const XMLString FEATURE_EVENTS;
	static const XMLString FEATURE_MUTATIONEVENTS;
	static const XMLString FEATURE_TRAVERSAL;
	static const XMLString VERSION_1_0;
	static const XMLString VERSION_2_0;	
};


} } // namespace Poco::XML


#endif // DOM_DOMImplementation_INCLUDED
