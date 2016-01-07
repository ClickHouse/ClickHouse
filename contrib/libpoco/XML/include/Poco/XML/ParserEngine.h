//
// ParserEngine.h
//
// $Id: //poco/1.4/XML/include/Poco/XML/ParserEngine.h#1 $
//
// Library: XML
// Package: XML
// Module:  ParserEngine
//
// Definition of the ParseEngine class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0


#ifndef XML_ParserEngine_INCLUDED
#define XML_ParserEngine_INCLUDED


#include "Poco/XML/XML.h"
#if defined(POCO_UNBUNDLED)
#include <expat.h>
#else
#include "Poco/XML/expat.h"
#endif
#include "Poco/XML/XMLString.h"
#include "Poco/XML/XMLStream.h"
#include "Poco/SAX/Locator.h"
#include "Poco/TextEncoding.h"
#include <map>
#include <vector>


namespace Poco {
namespace XML {


class InputSource;
class EntityResolver;
class DTDHandler;
class DeclHandler;
class ContentHandler;
class LexicalHandler;
class ErrorHandler;
class NamespaceStrategy;
class ContextLocator;


class XML_API ParserEngine: public Locator
	/// This class provides an object-oriented, stream-based, 
	/// low-level interface to the XML Parser Toolkit (expat).
	/// It is strongly recommended, that you use the
	/// SAX parser classes (which are based on this
	/// class) instead of this class, since they provide 
	/// a standardized, higher-level interface to the parser.
{
public:
	ParserEngine();
		/// Creates the parser engine.
		
	ParserEngine(const XMLString& encoding);
		/// Creates the parser engine and passes the encoding
		/// to the underlying parser.
		
	~ParserEngine();
		/// Destroys the parser.

	void setEncoding(const XMLString& encoding);
		/// Sets the encoding used by expat. The encoding must be
		/// set before parsing begins, otherwise it will be ignored.
		
	const XMLString& getEncoding() const;
		/// Returns the encoding used by expat.

	void addEncoding(const XMLString& name, Poco::TextEncoding* pEncoding);
		/// Adds an encoding to the parser.

	void setNamespaceStrategy(NamespaceStrategy* pStrategy);
		/// Sets the NamespaceStrategy used by the parser.
		/// The parser takes ownership of the strategy object
		/// and deletes it when it's no longer needed.
		/// The default is NoNamespacesStrategy.
		
	NamespaceStrategy* getNamespaceStrategy() const;
		/// Returns the NamespaceStrategy currently in use.

	void setExpandInternalEntities(bool flag = true);
		/// Enables/disables expansion of internal entities (enabled by
		/// default). If entity expansion is disabled, internal entities 
		/// are reported via the default handler.
		/// Must be set before parsing begins, otherwise it will be
		/// ignored.
		
	bool getExpandInternalEntities() const;
		/// Returns true if internal entities will be expanded automatically,
		/// which is the default.

	void setExternalGeneralEntities(bool flag = true);
		/// Enable or disable processing of external general entities.
		
	bool getExternalGeneralEntities() const;
		/// Returns true if external general entities will be processed; false otherwise.

	void setExternalParameterEntities(bool flag = true);
		/// Enable or disable processing of external parameter entities.
		
	bool getExternalParameterEntities() const;
		/// Returns true if external parameter entities will be processed; false otherwise.
		
	void setEntityResolver(EntityResolver* pResolver);
		/// Allow an application to register an entity resolver.

	EntityResolver* getEntityResolver() const;
		/// Return the current entity resolver.

	void setDTDHandler(DTDHandler* pDTDHandler);
		/// Allow an application to register a DTD event handler.

	DTDHandler* getDTDHandler() const;
		/// Return the current DTD handler.

	void setDeclHandler(DeclHandler* pDeclHandler);
		/// Allow an application to register a DTD declarations event handler.
		
	DeclHandler* getDeclHandler() const;
		/// Return the current DTD declarations handler.

	void setContentHandler(ContentHandler* pContentHandler);
		/// Allow an application to register a content event handler.

	ContentHandler* getContentHandler() const;
		/// Return the current content handler.

	void setLexicalHandler(LexicalHandler* pLexicalHandler);
		/// Allow an application to register a lexical event handler.
		
	LexicalHandler* getLexicalHandler() const;
		/// Return the current lexical handler.

	void setErrorHandler(ErrorHandler* pErrorHandler);
		/// Allow an application to register an error event handler.

	ErrorHandler* getErrorHandler() const;
		/// Return the current error handler.
		
	void setEnablePartialReads(bool flag = true);
		/// Enable or disable partial reads from the input source.
		///
		/// This is useful for parsing XML from a socket stream for
		/// a protocol like XMPP, where basically single elements 
		/// are read one at a time from the input source's stream, and
		/// following elements depend upon responses sent back to
		/// the peer.
		///
		/// Normally, the parser always reads blocks of PARSE_BUFFER_SIZE
		/// at a time, and blocks until a complete block has been read (or
		/// the end of the stream has been reached).
		/// This allows for efficient parsing of "complete" XML documents,
		/// but fails in a case such as XMPP, where only XML fragments
		/// are sent at a time.
		
	bool getEnablePartialReads() const;
		/// Returns true if partial reads are enabled (see
		/// setEnablePartialReads()), false otherwise.
	
	void parse(InputSource* pInputSource);
		/// Parse an XML document from the given InputSource.
		
	void parse(const char* pBuffer, std::size_t size);
		/// Parses an XML document from the given buffer.
	
	// Locator
	XMLString getPublicId() const;
		/// Return the public identifier for the current document event.
	
	XMLString getSystemId() const;
		/// Return the system identifier for the current document event.

	int getLineNumber() const;
		/// Return the line number where the current document event ends.

	int getColumnNumber() const;
		/// Return the column number where the current document event ends. 

protected:
	void init();
		/// initializes expat

	void parseByteInputStream(XMLByteInputStream& istr);
		/// Parses an entity from the given stream.

	void parseCharInputStream(XMLCharInputStream& istr);
		/// Parses an entity from the given stream.
		
	std::streamsize readBytes(XMLByteInputStream& istr, char* pBuffer, std::streamsize bufferSize);
		/// Reads at most bufferSize bytes from the given stream into the given buffer.

	std::streamsize readChars(XMLCharInputStream& istr, XMLChar* pBuffer, std::streamsize bufferSize);
		/// Reads at most bufferSize chars from the given stream into the given buffer.

	void handleError(int errorNo);
		/// Throws an XMLException with a message corresponding
		/// to the given Expat error code.

	void parseExternal(XML_Parser extParser, InputSource* pInputSource);
		/// Parse an XML document from the given InputSource.

	void parseExternalByteInputStream(XML_Parser extParser, XMLByteInputStream& istr);
		/// Parses an external entity from the given stream, with a separate parser.

	void parseExternalCharInputStream(XML_Parser extParser, XMLCharInputStream& istr);
		/// Parses an external entity from the given stream, with a separate parser.

	void pushContext(XML_Parser parser, InputSource* pInputSource);
		/// Pushes a new entry to the context stack.
		
	void popContext();
		/// Pops the top-most entry from the context stack.
	
	void resetContext();
		/// Resets and clears the context stack.

	const Locator& locator() const;
		/// Returns a locator denoting the current parse location.

	// expat handler procedures
	static void handleStartElement(void* userData, const XML_Char* name, const XML_Char** atts);
	static void handleEndElement(void* userData, const XML_Char* name);
	static void handleCharacterData(void* userData, const XML_Char* s, int len);
	static void handleProcessingInstruction(void* userData, const XML_Char* target, const XML_Char* data);
	static void handleDefault(void* userData, const XML_Char* s, int len);
	static void handleUnparsedEntityDecl(void* userData, const XML_Char* entityName, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId, const XML_Char* notationName);
	static void handleNotationDecl(void* userData, const XML_Char* notationName, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId);
	static int handleExternalEntityRef(XML_Parser parser, const XML_Char* openEntityNames, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId);
	static int handleUnknownEncoding(void* encodingHandlerData, const XML_Char* name, XML_Encoding* info);
	static void handleComment(void* userData, const XML_Char* data);
	static void handleStartCdataSection(void* userData);
	static void handleEndCdataSection(void* userData);
	static void handleStartNamespaceDecl(void* userData, const XML_Char* prefix, const XML_Char* uri);
	static void handleEndNamespaceDecl(void* userData, const XML_Char* prefix);
	static void handleStartDoctypeDecl(void* userData, const XML_Char* doctypeName, const XML_Char *systemId, const XML_Char* publicId, int hasInternalSubset);
	static void handleEndDoctypeDecl(void* userData);
	static void handleEntityDecl(void *userData, const XML_Char *entityName, int isParamEntity, const XML_Char *value, int valueLength, 
	                             const XML_Char *base, const XML_Char *systemId, const XML_Char *publicId, const XML_Char *notationName);
	static void handleExternalParsedEntityDecl(void* userData, const XML_Char* entityName, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId);
	static void handleInternalParsedEntityDecl(void* userData, const XML_Char* entityName, const XML_Char* replacementText, int replacementTextLength);
	static void handleSkippedEntity(void* userData, const XML_Char* entityName, int isParameterEntity);

	// encoding support
	static int convert(void *data, const char *s);
	
private:
	typedef std::map<XMLString, Poco::TextEncoding*> EncodingMap;
	typedef std::vector<ContextLocator*> ContextStack;
	
	XML_Parser _parser;
	char*      _pBuffer;
	bool       _encodingSpecified; 
	XMLString  _encoding;
	bool       _expandInternalEntities;
	bool       _externalGeneralEntities;
	bool       _externalParameterEntities;
	bool       _enablePartialReads;
	NamespaceStrategy* _pNamespaceStrategy;
	EncodingMap        _encodings;
	ContextStack       _context;
	
	EntityResolver* _pEntityResolver;
	DTDHandler*     _pDTDHandler;
	DeclHandler*    _pDeclHandler;
	ContentHandler* _pContentHandler;
	LexicalHandler* _pLexicalHandler;
	ErrorHandler*   _pErrorHandler;
	
	static const int PARSE_BUFFER_SIZE;
	static const XMLString EMPTY_STRING;
};


//
// inlines
//
inline const XMLString& ParserEngine::getEncoding() const
{
	return _encoding;
}


inline NamespaceStrategy* ParserEngine::getNamespaceStrategy() const
{
	return _pNamespaceStrategy;
}


inline bool ParserEngine::getExpandInternalEntities() const
{
	return _expandInternalEntities;
}


inline bool ParserEngine::getExternalGeneralEntities() const
{
	return _externalGeneralEntities;
}


inline bool ParserEngine::getExternalParameterEntities() const
{
	return _externalParameterEntities;
}


inline EntityResolver* ParserEngine::getEntityResolver() const
{
	return _pEntityResolver;
}


inline DTDHandler* ParserEngine::getDTDHandler() const
{
	return _pDTDHandler;
}


inline DeclHandler* ParserEngine::getDeclHandler() const
{
	return _pDeclHandler;
}


inline ContentHandler* ParserEngine::getContentHandler() const
{
	return _pContentHandler;
}


inline LexicalHandler* ParserEngine::getLexicalHandler() const
{
	return _pLexicalHandler;
}


inline ErrorHandler* ParserEngine::getErrorHandler() const
{
	return _pErrorHandler;
}


inline bool ParserEngine::getEnablePartialReads() const
{
	return _enablePartialReads;
}


} } // namespace Poco::XML


#endif // XML_ParserEngine_INCLUDED
