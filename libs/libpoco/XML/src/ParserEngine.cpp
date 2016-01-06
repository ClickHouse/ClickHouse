//
// ParserEngine.cpp
//
// $Id: //poco/1.4/XML/src/ParserEngine.cpp#2 $
//
// Library: XML
// Package: XML
// Module:  ParserEngine
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/ParserEngine.h"
#include "Poco/XML/NamespaceStrategy.h"
#include "Poco/XML/XMLException.h"
#include "Poco/SAX/EntityResolver.h"
#include "Poco/SAX/EntityResolverImpl.h"
#include "Poco/SAX/DTDHandler.h"
#include "Poco/SAX/DeclHandler.h"
#include "Poco/SAX/ContentHandler.h"
#include "Poco/SAX/LexicalHandler.h"
#include "Poco/SAX/ErrorHandler.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/SAX/Locator.h"
#include "Poco/SAX/LocatorImpl.h"
#include "Poco/SAX/SAXException.h"
#include "Poco/URI.h"
#include <cstring>


using Poco::URI;
using Poco::TextEncoding;


namespace Poco {
namespace XML {


class ContextLocator: public Locator
{
public:
	ContextLocator(XML_Parser parser, const XMLString& publicId, const XMLString& systemId):
		_parser(parser),
		_publicId(publicId),
		_systemId(systemId)
	{
	}
		
	~ContextLocator()
	{
	}
		
	XMLString getPublicId() const
	{
		return _publicId;
	}
		
	XMLString getSystemId() const
	{
		return _systemId;
	}

	int getLineNumber() const
	{
		return XML_GetCurrentLineNumber(_parser);
	}
		
	int getColumnNumber() const
	{
		return XML_GetCurrentColumnNumber(_parser);
	}
		
private:
	XML_Parser _parser;
	XMLString  _publicId;
	XMLString  _systemId;
};


const int ParserEngine::PARSE_BUFFER_SIZE = 4096;
const XMLString ParserEngine::EMPTY_STRING;


ParserEngine::ParserEngine():
	_parser(0),
	_pBuffer(0),
	_encodingSpecified(false),
	_expandInternalEntities(true),
	_externalGeneralEntities(false),
	_externalParameterEntities(false),
	_enablePartialReads(false),
	_pNamespaceStrategy(new NoNamespacesStrategy()),
	_pEntityResolver(0),
	_pDTDHandler(0),
	_pDeclHandler(0),
	_pContentHandler(0),
	_pLexicalHandler(0),
	_pErrorHandler(0)
{
}


ParserEngine::ParserEngine(const XMLString& encoding):
	_parser(0),
	_pBuffer(0),
	_encodingSpecified(true),
	_encoding(encoding),
	_expandInternalEntities(true),
	_externalGeneralEntities(false),
	_externalParameterEntities(false),
	_enablePartialReads(false),
	_pNamespaceStrategy(new NoNamespacesStrategy()),
	_pEntityResolver(0),
	_pDTDHandler(0),
	_pDeclHandler(0),
	_pContentHandler(0),
	_pLexicalHandler(0),
	_pErrorHandler(0)
{
}


ParserEngine::~ParserEngine()
{
	resetContext();
	if (_parser) XML_ParserFree(_parser);
	delete [] _pBuffer;
	delete _pNamespaceStrategy;
}


void ParserEngine::setEncoding(const XMLString& encoding)
{
	_encoding          = encoding;
	_encodingSpecified = true;
}


void ParserEngine::addEncoding(const XMLString& name, TextEncoding* pEncoding)
{
	poco_check_ptr (pEncoding);

	if (_encodings.find(name) == _encodings.end())
		_encodings[name] = pEncoding;
	else
		throw XMLException("Encoding already defined");	
}


void ParserEngine::setNamespaceStrategy(NamespaceStrategy* pStrategy)
{
	poco_check_ptr (pStrategy);
	
	delete _pNamespaceStrategy;
	_pNamespaceStrategy = pStrategy;
}


void ParserEngine::setExpandInternalEntities(bool flag)
{
	_expandInternalEntities = flag;
}


void ParserEngine::setExternalGeneralEntities(bool flag)
{
	_externalGeneralEntities = flag;
}


void ParserEngine::setExternalParameterEntities(bool flag)
{
	_externalParameterEntities = flag;
}


void ParserEngine::setEntityResolver(EntityResolver* pResolver)
{
	_pEntityResolver = pResolver;
}


void ParserEngine::setDTDHandler(DTDHandler* pDTDHandler)
{
	_pDTDHandler = pDTDHandler;
}


void ParserEngine::setDeclHandler(DeclHandler* pDeclHandler)
{
	_pDeclHandler = pDeclHandler;
}


void ParserEngine::setContentHandler(ContentHandler* pContentHandler)
{
	_pContentHandler = pContentHandler;
}


void ParserEngine::setLexicalHandler(LexicalHandler* pLexicalHandler)
{
	_pLexicalHandler = pLexicalHandler;
}


void ParserEngine::setErrorHandler(ErrorHandler* pErrorHandler)
{
	_pErrorHandler = pErrorHandler;
}


void ParserEngine::setEnablePartialReads(bool flag)
{
	_enablePartialReads = flag;
}


void ParserEngine::parse(InputSource* pInputSource)
{
	init();
	resetContext();
	pushContext(_parser, pInputSource);
	if (_pContentHandler) _pContentHandler->setDocumentLocator(this);
	if (_pContentHandler) _pContentHandler->startDocument();
	if (pInputSource->getCharacterStream())
		parseCharInputStream(*pInputSource->getCharacterStream());
	else if (pInputSource->getByteStream())
		parseByteInputStream(*pInputSource->getByteStream());
	else throw XMLException("Input source has no stream");
	if (_pContentHandler) _pContentHandler->endDocument();
	popContext();
}


void ParserEngine::parse(const char* pBuffer, std::size_t size)
{
	init();
	resetContext();
	InputSource src;
	pushContext(_parser, &src);
	if (_pContentHandler) _pContentHandler->setDocumentLocator(this);
	if (_pContentHandler) _pContentHandler->startDocument();
	std::size_t processed = 0;
	while (processed < size)
	{
		const int bufferSize = processed + PARSE_BUFFER_SIZE < size ? PARSE_BUFFER_SIZE : size - processed;
		if (!XML_Parse(_parser, pBuffer + processed, bufferSize, 0))
			handleError(XML_GetErrorCode(_parser));
		processed += bufferSize;
	}
	if (!XML_Parse(_parser, pBuffer+processed, 0, 1))
		handleError(XML_GetErrorCode(_parser));
	if (_pContentHandler) _pContentHandler->endDocument();
	popContext();
}


void ParserEngine::parseByteInputStream(XMLByteInputStream& istr)
{
	std::streamsize n = readBytes(istr, _pBuffer, PARSE_BUFFER_SIZE);
	while (n > 0)
	{
		if (!XML_Parse(_parser, _pBuffer, static_cast<int>(n), 0))
			handleError(XML_GetErrorCode(_parser));
		if (istr.good())
			n = readBytes(istr, _pBuffer, PARSE_BUFFER_SIZE);
		else 
			n = 0;
	}
	if (!XML_Parse(_parser, _pBuffer, 0, 1))
		handleError(XML_GetErrorCode(_parser));
}


void ParserEngine::parseCharInputStream(XMLCharInputStream& istr)
{
	std::streamsize n = readChars(istr, reinterpret_cast<XMLChar*>(_pBuffer), PARSE_BUFFER_SIZE/sizeof(XMLChar));
	while (n > 0)
	{
		if (!XML_Parse(_parser, _pBuffer, static_cast<int>(n*sizeof(XMLChar)), 0))
			handleError(XML_GetErrorCode(_parser));
		if (istr.good())
			n = readChars(istr, reinterpret_cast<XMLChar*>(_pBuffer), PARSE_BUFFER_SIZE/sizeof(XMLChar));
		else 
			n = 0;
	}
	if (!XML_Parse(_parser, _pBuffer, 0, 1))
		handleError(XML_GetErrorCode(_parser));
}


void ParserEngine::parseExternal(XML_Parser extParser, InputSource* pInputSource)
{
	pushContext(extParser, pInputSource);
	if (pInputSource->getCharacterStream())
		parseExternalCharInputStream(extParser, *pInputSource->getCharacterStream());
	else if (pInputSource->getByteStream())
		parseExternalByteInputStream(extParser, *pInputSource->getByteStream());
	else throw XMLException("Input source has no stream");
	popContext();
}


void ParserEngine::parseExternalByteInputStream(XML_Parser extParser, XMLByteInputStream& istr)
{
	char *pBuffer = new char[PARSE_BUFFER_SIZE];
	try
	{
		std::streamsize n = readBytes(istr, pBuffer, PARSE_BUFFER_SIZE);
		while (n > 0)
		{
			if (!XML_Parse(extParser, pBuffer, static_cast<int>(n), 0))
				handleError(XML_GetErrorCode(extParser));
			if (istr.good())
				n = readBytes(istr, pBuffer, PARSE_BUFFER_SIZE);
			else 
				n = 0;
		}
		if (!XML_Parse(extParser, pBuffer, 0, 1))
			handleError(XML_GetErrorCode(extParser));
	}
	catch (...)
	{
		delete [] pBuffer;
		throw;
	}
	delete [] pBuffer;
}


void ParserEngine::parseExternalCharInputStream(XML_Parser extParser, XMLCharInputStream& istr)
{
	XMLChar *pBuffer = new XMLChar[PARSE_BUFFER_SIZE/sizeof(XMLChar)];
	try
	{
		std::streamsize n = readChars(istr, pBuffer, PARSE_BUFFER_SIZE/sizeof(XMLChar));
		while (n > 0)
		{
			if (!XML_Parse(extParser, reinterpret_cast<char*>(pBuffer), static_cast<int>(n*sizeof(XMLChar)), 0))
				handleError(XML_GetErrorCode(extParser));
			if (istr.good())
				n = readChars(istr, pBuffer, static_cast<int>(PARSE_BUFFER_SIZE/sizeof(XMLChar)));
			else 
				n = 0;
		}
		if (!XML_Parse(extParser, reinterpret_cast<char*>(pBuffer), 0, 1))
			handleError(XML_GetErrorCode(extParser));
	}
	catch (...)
	{
		delete [] pBuffer;
		throw;
	}
	delete [] pBuffer;
}


std::streamsize ParserEngine::readBytes(XMLByteInputStream& istr, char* pBuffer, std::streamsize bufferSize)
{
	if (_enablePartialReads)
	{
		istr.read(pBuffer, 1);
		if (istr.gcount() == 1)
		{
			std::streamsize n = istr.readsome(pBuffer + 1, bufferSize - 1);
			return n + 1;
		}
		else return 0;
	}
	else
	{
		istr.read(pBuffer, bufferSize);
		return istr.gcount();
	}
}


std::streamsize ParserEngine::readChars(XMLCharInputStream& istr, XMLChar* pBuffer, std::streamsize bufferSize)
{
	if (_enablePartialReads)
	{
		istr.read(pBuffer, 1);
		if (istr.gcount() == 1)
		{
			std::streamsize n = istr.readsome(pBuffer + 1, bufferSize - 1);
			return n + 1;
		}
		else return 0;
	}
	else
	{
		istr.read(pBuffer, bufferSize);
		return istr.gcount();
	}
}


XMLString ParserEngine::getPublicId() const
{
	return locator().getPublicId();
}


XMLString ParserEngine::getSystemId() const
{
	return locator().getSystemId();
}


int ParserEngine::getLineNumber() const
{
	return locator().getLineNumber();
}


int ParserEngine::getColumnNumber() const
{
	return locator().getColumnNumber();
}


namespace
{
	static LocatorImpl nullLocator;
}


const Locator& ParserEngine::locator() const
{
	if (_context.empty())
		return nullLocator;
	else
		return *_context.back();
}


void ParserEngine::init()
{
	if (_parser)
		XML_ParserFree(_parser);

	if (!_pBuffer)
		_pBuffer  = new char[PARSE_BUFFER_SIZE];

	if (dynamic_cast<NoNamespacePrefixesStrategy*>(_pNamespaceStrategy))
	{
		_parser = XML_ParserCreateNS(_encodingSpecified ? _encoding.c_str() : 0, '\t');
		XML_SetNamespaceDeclHandler(_parser, handleStartNamespaceDecl, handleEndNamespaceDecl);
	}
	else if (dynamic_cast<NamespacePrefixesStrategy*>(_pNamespaceStrategy))
	{
		_parser = XML_ParserCreateNS(_encodingSpecified ? _encoding.c_str() : 0, '\t');
		XML_SetReturnNSTriplet(_parser, 1);
		XML_SetNamespaceDeclHandler(_parser, handleStartNamespaceDecl, handleEndNamespaceDecl);
	}
	else
	{
		_parser = XML_ParserCreate(_encodingSpecified ? _encoding.c_str() : 0);
	}

	XML_SetUserData(_parser, this);
	XML_SetElementHandler(_parser, handleStartElement, handleEndElement);
	XML_SetCharacterDataHandler(_parser, handleCharacterData);
	XML_SetProcessingInstructionHandler(_parser, handleProcessingInstruction);
	if (_expandInternalEntities)
		XML_SetDefaultHandlerExpand(_parser, handleDefault);
	else
		XML_SetDefaultHandler(_parser, handleDefault);
	XML_SetUnparsedEntityDeclHandler(_parser, handleUnparsedEntityDecl);
	XML_SetNotationDeclHandler(_parser, handleNotationDecl);
	XML_SetExternalEntityRefHandler(_parser, handleExternalEntityRef);
	XML_SetCommentHandler(_parser, handleComment);
	XML_SetCdataSectionHandler(_parser, handleStartCdataSection, handleEndCdataSection);
	XML_SetDoctypeDeclHandler(_parser, handleStartDoctypeDecl, handleEndDoctypeDecl);
	XML_SetEntityDeclHandler(_parser, handleEntityDecl);
	XML_SetSkippedEntityHandler(_parser, handleSkippedEntity);
	XML_SetParamEntityParsing(_parser, _externalParameterEntities ? XML_PARAM_ENTITY_PARSING_ALWAYS : XML_PARAM_ENTITY_PARSING_NEVER);
	XML_SetUnknownEncodingHandler(_parser, handleUnknownEncoding, this);
}


void ParserEngine::handleError(int errorNo)
{
	try
	{
		switch (errorNo)
		{
		case XML_ERROR_NO_MEMORY:
			throw XMLException("No memory");
		case XML_ERROR_SYNTAX:
			throw SAXParseException("Syntax error", locator());
		case XML_ERROR_NO_ELEMENTS:
			throw SAXParseException("No element found", locator());
		case XML_ERROR_INVALID_TOKEN:
			throw SAXParseException("Invalid token", locator());
		case XML_ERROR_UNCLOSED_TOKEN:
			throw SAXParseException("Unclosed token", locator());
		case XML_ERROR_PARTIAL_CHAR:
			throw SAXParseException("Partial character", locator());
		case XML_ERROR_TAG_MISMATCH:
			throw SAXParseException("Tag mismatch", locator());
		case XML_ERROR_DUPLICATE_ATTRIBUTE:
			throw SAXParseException("Duplicate attribute", locator());
		case XML_ERROR_JUNK_AFTER_DOC_ELEMENT:
			throw SAXParseException("Junk after document element", locator());
		case XML_ERROR_PARAM_ENTITY_REF:
			throw SAXParseException("Illegal parameter entity reference", locator());
		case XML_ERROR_UNDEFINED_ENTITY:
			throw SAXParseException("Undefined entity", locator());
		case XML_ERROR_RECURSIVE_ENTITY_REF:
			throw SAXParseException("Recursive entity reference", locator());
		case XML_ERROR_ASYNC_ENTITY:
			throw SAXParseException("Asynchronous entity", locator());
		case XML_ERROR_BAD_CHAR_REF:
			throw SAXParseException("Reference to invalid character number", locator());
		case XML_ERROR_BINARY_ENTITY_REF:
			throw SAXParseException("Reference to binary entity", locator());
		case XML_ERROR_ATTRIBUTE_EXTERNAL_ENTITY_REF:
			throw SAXParseException("Reference to external entity in attribute", locator());
		case XML_ERROR_MISPLACED_XML_PI:
			throw SAXParseException("XML processing instruction not at start of external entity", locator());
		case XML_ERROR_UNKNOWN_ENCODING:
			throw SAXParseException("Unknown encoding", locator());
		case XML_ERROR_INCORRECT_ENCODING:
			throw SAXParseException("Encoding specified in XML declaration is incorrect", locator());
		case XML_ERROR_UNCLOSED_CDATA_SECTION:
			throw SAXParseException("Unclosed CDATA section", locator());
		case XML_ERROR_EXTERNAL_ENTITY_HANDLING:
			throw SAXParseException("Error in processing external entity reference", locator());
		case XML_ERROR_NOT_STANDALONE:
			throw SAXParseException("Document is not standalone", locator());
		case XML_ERROR_UNEXPECTED_STATE:
			throw SAXParseException("Unexpected parser state - please send a bug report", locator());		
		case XML_ERROR_ENTITY_DECLARED_IN_PE:
			throw SAXParseException("Entity declared in parameter entity", locator());
		case XML_ERROR_FEATURE_REQUIRES_XML_DTD:
			throw SAXParseException("Requested feature requires XML_DTD support in Expat", locator());
		case XML_ERROR_CANT_CHANGE_FEATURE_ONCE_PARSING:
			throw SAXParseException("Cannot change setting once parsing has begun", locator());
		case XML_ERROR_UNBOUND_PREFIX:
			throw SAXParseException("Unbound prefix", locator());
		case XML_ERROR_UNDECLARING_PREFIX:
			throw SAXParseException("Must not undeclare prefix", locator());
		case XML_ERROR_INCOMPLETE_PE:
			throw SAXParseException("Incomplete markup in parameter entity", locator());
		case XML_ERROR_XML_DECL:
			throw SAXParseException("XML declaration not well-formed", locator());
		case XML_ERROR_TEXT_DECL:
			throw SAXParseException("Text declaration not well-formed", locator());
		case XML_ERROR_PUBLICID:
			throw SAXParseException("Illegal character(s) in public identifier", locator());
		case XML_ERROR_SUSPENDED:
			throw SAXParseException("Parser suspended", locator());
		case XML_ERROR_NOT_SUSPENDED:
			throw SAXParseException("Parser not suspended", locator());
		case XML_ERROR_ABORTED:
			throw SAXParseException("Parsing aborted", locator());
		case XML_ERROR_FINISHED:
			throw SAXParseException("Parsing finished", locator());
		case XML_ERROR_SUSPEND_PE:
			throw SAXParseException("Cannot suspend in external parameter entity", locator());
		}
		throw XMLException("Unknown Expat error code");
	}
	catch (SAXException& exc)
	{
		if (_pErrorHandler) _pErrorHandler->error(exc);
		throw;
	}
	catch (Poco::Exception& exc)
	{
		if (_pErrorHandler) _pErrorHandler->fatalError(SAXParseException("Fatal error", locator(), exc));
		throw;
	}
}	


void ParserEngine::pushContext(XML_Parser parser, InputSource* pInputSource)
{
	ContextLocator* pLocator = new ContextLocator(parser, pInputSource->getPublicId(), pInputSource->getSystemId());
	_context.push_back(pLocator);
}


void ParserEngine::popContext()
{
	poco_assert (!_context.empty());
	delete _context.back();
	_context.pop_back();
}


void ParserEngine::resetContext()
{
	for (ContextStack::iterator it = _context.begin(); it != _context.end(); ++it)
	{
		delete *it;
	}
	_context.clear();
}


void ParserEngine::handleStartElement(void* userData, const XML_Char* name, const XML_Char** atts)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);
	
	if (pThis->_pContentHandler)
	{
		try
		{
			pThis->_pNamespaceStrategy->startElement(name, atts, XML_GetSpecifiedAttributeCount(pThis->_parser)/2, pThis->_pContentHandler);	
		}
		catch (XMLException& exc)
		{
			throw SAXParseException(exc.message(), pThis->locator());
		}
	}
}


void ParserEngine::handleEndElement(void* userData, const XML_Char* name)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);
	
	if (pThis->_pContentHandler)
	{
		try
		{
			pThis->_pNamespaceStrategy->endElement(name, pThis->_pContentHandler);	
		}
		catch (XMLException& exc)
		{
			throw SAXParseException(exc.message(), pThis->locator());
		}
	}
}


void ParserEngine::handleCharacterData(void* userData, const XML_Char* s, int len)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);
	
	if (pThis->_pContentHandler)
		pThis->_pContentHandler->characters(s, 0, len);
}


void ParserEngine::handleProcessingInstruction(void* userData, const XML_Char* target, const XML_Char* data)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);
	
	if (pThis->_pContentHandler)
		pThis->_pContentHandler->processingInstruction(target, data);
}


void ParserEngine::handleDefault(void* userData, const XML_Char* s, int len)
{
}


void ParserEngine::handleUnparsedEntityDecl(void* userData, const XML_Char* entityName, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId, const XML_Char* notationName)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);
	
	XMLString pubId;
	if (publicId) pubId.assign(publicId);
	if (pThis->_pDTDHandler) 
		pThis->_pDTDHandler->unparsedEntityDecl(entityName, publicId ? &pubId : 0, systemId, notationName);
}


void ParserEngine::handleNotationDecl(void* userData, const XML_Char* notationName, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);
	
	XMLString pubId;
	if (publicId) pubId.assign(publicId);
	XMLString sysId;
	if (systemId) sysId.assign(systemId);
	if (pThis->_pDTDHandler) 
		pThis->_pDTDHandler->notationDecl(notationName, publicId ? &pubId : 0, systemId ? &sysId : 0);
}


int ParserEngine::handleExternalEntityRef(XML_Parser parser, const XML_Char* context, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(XML_GetUserData(parser));

	if (!context && !pThis->_externalParameterEntities) return XML_STATUS_ERROR;
	if (context && !pThis->_externalGeneralEntities) return XML_STATUS_ERROR;

	InputSource* pInputSource = 0;
	EntityResolver* pEntityResolver = 0;
	EntityResolverImpl defaultResolver;

	XMLString sysId(systemId);
	XMLString pubId;
	if (publicId) pubId.assign(publicId);
	
	URI uri(fromXMLString(pThis->_context.back()->getSystemId()));
	uri.resolve(fromXMLString(sysId));

	if (pThis->_pEntityResolver)
	{
		pEntityResolver = pThis->_pEntityResolver;
		pInputSource = pEntityResolver->resolveEntity(publicId ? &pubId : 0, toXMLString(uri.toString()));
	}
	if (!pInputSource && pThis->_externalGeneralEntities)
	{
		pEntityResolver = &defaultResolver;
		pInputSource = pEntityResolver->resolveEntity(publicId ? &pubId : 0, toXMLString(uri.toString()));
	}

	if (pInputSource)
	{
		XML_Parser extParser = XML_ExternalEntityParserCreate(pThis->_parser, context, 0);
		try
		{
			pThis->parseExternal(extParser, pInputSource);
		}
		catch (XMLException&)
		{
			pEntityResolver->releaseInputSource(pInputSource);
			XML_ParserFree(extParser);
			throw;
		}
		pEntityResolver->releaseInputSource(pInputSource);
		XML_ParserFree(extParser);
		return XML_STATUS_OK;
	}
	else return XML_STATUS_ERROR;
}


int ParserEngine::handleUnknownEncoding(void* encodingHandlerData, const XML_Char* name, XML_Encoding* info)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(encodingHandlerData);
	
	XMLString encoding(name);
	TextEncoding* knownEncoding = 0;

	EncodingMap::const_iterator it = pThis->_encodings.find(encoding);
	if (it != pThis->_encodings.end())
		knownEncoding = it->second;
	else
		knownEncoding = Poco::TextEncoding::find(fromXMLString(encoding));

	if (knownEncoding)
	{
		const TextEncoding::CharacterMap& map = knownEncoding->characterMap();
		for (int i = 0; i < 256; ++i)
			info->map[i] = map[i];
			
		info->data    = knownEncoding;
		info->convert = &ParserEngine::convert;
		info->release = 0;
		return XML_STATUS_OK;
	}
	else return XML_STATUS_ERROR;
}


void ParserEngine::handleComment(void* userData, const XML_Char* data)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

#if defined(XML_UNICODE_WCHAR_T)
	if (pThis->_pLexicalHandler)
		pThis->_pLexicalHandler->comment(data, 0, (int) std::wcslen(data));
#else
	if (pThis->_pLexicalHandler)
		pThis->_pLexicalHandler->comment(data, 0, (int) std::strlen(data));
#endif
}


void ParserEngine::handleStartCdataSection(void* userData)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	if (pThis->_pLexicalHandler)
		pThis->_pLexicalHandler->startCDATA();
}


void ParserEngine::handleEndCdataSection(void* userData)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	if (pThis->_pLexicalHandler)
		pThis->_pLexicalHandler->endCDATA();
}


void ParserEngine::handleStartNamespaceDecl(void* userData, const XML_Char* prefix, const XML_Char* uri)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	if (pThis->_pContentHandler)
		pThis->_pContentHandler->startPrefixMapping((prefix ? XMLString(prefix) : EMPTY_STRING), (uri ? XMLString(uri) : EMPTY_STRING));
}


void ParserEngine::handleEndNamespaceDecl(void* userData, const XML_Char* prefix)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	if (pThis->_pContentHandler)
		pThis->_pContentHandler->endPrefixMapping(prefix ? XMLString(prefix) : EMPTY_STRING);
}


void ParserEngine::handleStartDoctypeDecl(void* userData, const XML_Char* doctypeName, const XML_Char *systemId, const XML_Char* publicId, int hasInternalSubset)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	if (pThis->_pLexicalHandler)
	{
		XMLString sysId = systemId ? XMLString(systemId) : EMPTY_STRING;
		XMLString pubId = publicId ? XMLString(publicId) : EMPTY_STRING;
		pThis->_pLexicalHandler->startDTD(doctypeName, pubId, sysId);
	}
}


void ParserEngine::handleEndDoctypeDecl(void* userData)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	if (pThis->_pLexicalHandler)
		pThis->_pLexicalHandler->endDTD();
}


void ParserEngine::handleEntityDecl(void *userData, const XML_Char *entityName, int isParamEntity, const XML_Char *value, int valueLength, 
	                                const XML_Char *base, const XML_Char *systemId, const XML_Char *publicId, const XML_Char *notationName)
{
	if (value)
		handleInternalParsedEntityDecl(userData, entityName, value, valueLength);
	else
		handleExternalParsedEntityDecl(userData, entityName, base, systemId, publicId);
}


void ParserEngine::handleExternalParsedEntityDecl(void* userData, const XML_Char* entityName, const XML_Char* base, const XML_Char* systemId, const XML_Char* publicId)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	XMLString pubId;
	if (publicId) pubId.assign(publicId);
	if (pThis->_pDeclHandler)
		pThis->_pDeclHandler->externalEntityDecl(entityName, publicId ? &pubId : 0, systemId);
}


void ParserEngine::handleInternalParsedEntityDecl(void* userData, const XML_Char* entityName, const XML_Char* replacementText, int replacementTextLength)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);

	XMLString replText(replacementText, replacementTextLength);
	if (pThis->_pDeclHandler)
		pThis->_pDeclHandler->internalEntityDecl(entityName, replText);
}


void ParserEngine::handleSkippedEntity(void* userData, const XML_Char* entityName, int isParameterEntity)
{
	ParserEngine* pThis = reinterpret_cast<ParserEngine*>(userData);
	
	if (pThis->_pContentHandler)
		pThis->_pContentHandler->skippedEntity(entityName);
}


int ParserEngine::convert(void* data, const char* s)
{
	TextEncoding* pEncoding = reinterpret_cast<TextEncoding*>(data);
	return pEncoding->convert((const unsigned char*) s);
}


} } // namespace Poco::XML
