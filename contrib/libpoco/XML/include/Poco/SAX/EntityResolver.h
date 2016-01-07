//
// EntityResolver.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/EntityResolver.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX EntityResolver Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_EntityResolver_INCLUDED
#define SAX_EntityResolver_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"


namespace Poco {
namespace XML {


class InputSource;


class XML_API EntityResolver
	/// If a SAX application needs to implement customized handling for external entities, 
	/// it must implement this interface and register an instance with the SAX driver using 
	/// the setEntityResolver method.
	/// 
	/// The XML reader will then allow the application to intercept any external entities 
	/// (including the external DTD subset and external parameter entities, if any) before 
	/// including them.
	/// 
	/// Many SAX applications will not need to implement this interface, but it will be 
	/// especially useful for applications that build XML documents from databases or other 
	/// specialised input sources, or for applications that use URI types other than URLs.
	///
	/// The application can also use this interface to redirect system identifiers to local 
	/// URIs or to look up replacements in a catalog (possibly by using the public identifier).
{
public:
	virtual InputSource* resolveEntity(const XMLString* publicId, const XMLString& systemId) = 0;
		/// Allow the application to resolve external entities.
		/// 
		/// The parser will call this method before opening any external entity except the 
		/// top-level document entity. Such entities include the external DTD subset and 
		/// external parameter entities referenced within the DTD (in either case, only 
		/// if the parser reads external parameter entities), and external general entities 
		/// referenced within the document element (if the parser reads external general entities). 
		/// The application may request that the parser locate the entity itself, that it use an 
		/// alternative URI, or that it use data provided by the application (as a character or 
		/// byte input stream).
		/// 
		/// Application writers can use this method to redirect external system identifiers to 
		/// secure and/or local URIs, to look up public identifiers in a catalogue, or to read an 
		/// entity from a database or other input source (including, for example, a dialog box). 
		/// Neither XML nor SAX specifies a preferred policy for using public or system IDs to resolve 
		/// resources. However, SAX specifies how to interpret any InputSource returned by this method, 
		/// and that if none is returned, then the system ID will be dereferenced as a URL.
		/// 
		/// If the system identifier is a URL, the SAX parser must resolve it fully before reporting it to 
		/// the application.
		/// 
		/// Note that publicId maybe null, therefore we pass a pointer rather than a reference.

	virtual void releaseInputSource(InputSource* pSource) = 0;
		/// This is a non-standard extension to SAX!
		/// Called by the parser when the input source returned by ResolveEntity is
		/// no longer needed. Should free any resources used by the input source.

protected:
	virtual ~EntityResolver();
};


} } // namespace Poco::XML


#endif // SAX_EntityResolver_INCLUDED
