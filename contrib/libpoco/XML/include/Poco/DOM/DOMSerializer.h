//
// DOMSerializer.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/DOMSerializer.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOMSerializer
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_DOMSerializer_INCLUDED
#define DOM_DOMSerializer_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/SAX/XMLReader.h"


namespace Poco {
namespace XML {


class Node;
class Element;
class Text;
class Comment;
class ProcessingInstruction;
class Entity;
class CDATASection;
class Notation;
class Document;
class DocumentType;
class DocumentFragment;
class DeclHandler;
class LexicalHandler;


class XML_API DOMSerializer: public XMLReader
	/// The DOMSerializer serializes a DOM document
	/// into a sequence of SAX events which are
	/// reported to the registered SAX event
	/// handlers.
	///
	/// The DOMWriter uses a DOMSerializer with an
	/// XMLWriter to serialize a DOM document into
	/// textual XML.
{
public:
	DOMSerializer();
		/// Creates the DOMSerializer.
		
	~DOMSerializer();
		/// Destroys the DOMSerializer.
		
	void serialize(const Node* pNode);
		/// Serializes a DOM node and its children
		/// into a sequence of SAX events, which are
		/// reported to the registered SAX event
		/// handlers.

	// XMLReader
	void setEntityResolver(EntityResolver* pResolver);
	EntityResolver* getEntityResolver() const;
	void setDTDHandler(DTDHandler* pDTDHandler);
	DTDHandler* getDTDHandler() const;
	void setContentHandler(ContentHandler* pContentHandler);
	ContentHandler* getContentHandler() const;
	void setErrorHandler(ErrorHandler* pErrorHandler);
	ErrorHandler* getErrorHandler() const;

	void setFeature(const XMLString& featureId, bool state);
	bool getFeature(const XMLString& featureId) const;
	void setProperty(const XMLString& propertyId, const XMLString& value);
	void setProperty(const XMLString& propertyId, void* value);
	void* getProperty(const XMLString& propertyId) const;

protected:
	void parse(InputSource* pSource);
		/// The DOMSerializer cannot parse an InputSource,
		/// so this method simply throws an XMLException when invoked.
		
	void parse(const XMLString& systemId);
		/// The DOMSerializer cannot parse from a system identifier,
		/// so this method simply throws an XMLException when invoked.

	void parseMemoryNP(const char* xml, std::size_t size);
		/// The DOMSerializer cannot parse from a system identifier,
		/// so this method simply throws an XMLException when invoked.

	void iterate(const Node* pNode) const;
	void handleNode(const Node* pNode) const;
	void handleElement(const Element* pElement) const;
	void handleCharacterData(const Text* pText) const;
	void handleComment(const Comment* pComment) const;
	void handlePI(const ProcessingInstruction* pPI) const;
	void handleCDATASection(const CDATASection* pCDATA) const;
	void handleDocument(const Document* pDocument) const;
	void handleDocumentType(const DocumentType* pDocumentType) const;
	void handleFragment(const DocumentFragment* pFragment) const;
	void handleNotation(const Notation* pNotation) const;
	void handleEntity(const Entity* pEntity) const;

private:
	EntityResolver* _pEntityResolver;
	DTDHandler*     _pDTDHandler;
	ContentHandler* _pContentHandler;
	ErrorHandler*   _pErrorHandler;
	DeclHandler*    _pDeclHandler;
	LexicalHandler* _pLexicalHandler;
	
	static const XMLString CDATA;
};


} } // namespace Poco::XML


#endif // DOM_DOMSerializer_INCLUDED
