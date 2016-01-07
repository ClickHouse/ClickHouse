//
// InputSource.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/InputSource.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX InputSource - A single input source for an XML entity.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_InputSource_INCLUDED
#define SAX_InputSource_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/XML/XMLString.h"
#include "Poco/XML/XMLStream.h"


namespace Poco {
namespace XML {


class XML_API InputSource
	/// This class allows a SAX application to encapsulate information about an input 
	/// source in a single object, which may include a public identifier, a system 
	/// identifier, a byte stream (possibly with a specified encoding), and/or a character 
	/// stream.
	/// 
	/// There are two places that the application can deliver an input source to the 
	/// parser: as the argument to the Parser.parse method, or as the return value of the 
	/// EntityResolver::resolveEntity() method.
	/// 
	/// The SAX parser will use the InputSource object to determine how to read XML input. 
	/// If there is a character stream available, the parser will read that stream directly, 
	/// disregarding any text encoding declaration found in that stream. If there is no character 
	/// stream, but there is a byte stream, the parser will use that byte stream, using the 
	/// encoding specified in the InputSource or else (if no encoding is specified) autodetecting 
	/// the character encoding using an algorithm such as the one in the XML specification. 
	/// If neither a character stream nor a byte stream is available, the parser will attempt 
	/// to open a URI connection to the resource identified by the system identifier.
	/// 
	/// An InputSource object belongs to the application: the SAX parser shall never modify it in 
	/// any way (it may modify a copy if necessary). However, standard processing of both byte and 
	/// character streams is to close them on as part of end-of-parse cleanup, so applications should 
	/// not attempt to re-use such streams after they have been handed to a parser.
{
public:
	InputSource();
		/// Zero-argument default constructor.
		
	InputSource(const XMLString& systemId);
		/// Creates a new input source with a system identifier.
		/// Applications may use setPublicId to include a public identifier as well, 
		/// or setEncoding to specify the character encoding, if known.
		/// 
		/// If the system identifier is a URL, it must be fully resolved (it may not 
		/// be a relative URL).
		
	InputSource(XMLByteInputStream& istr);
		/// Creates a new input source with a byte stream.
		/// 
		/// Application writers should use setSystemId() to provide a base for resolving
		/// relative URIs, may use setPublicId to include a public identifier, and may use 
		/// setEncoding to specify the object's character encoding.

	~InputSource();
		/// Destroys the InputSource.

	void setPublicId(const XMLString& publicId);
		/// Set the public identifier for this input source.
		///
		/// The public identifier is always optional: if the application writer includes one, 
		/// it will be provided as part of the location information.
		
	void setSystemId(const XMLString& systemId);
		/// Set the system identifier for this input source.
		///
		/// The system identifier is optional if there is a byte stream or a character stream, 
		/// but it is still useful to provide one, since the application can use it to resolve 
		/// relative URIs and can include it in error messages and warnings (the parser will 
		/// attempt to open a connection to the URI only if there is no byte stream or character 
		/// stream specified).
		/// 
		/// If the application knows the character encoding of the object pointed to by the system 
		/// identifier, it can register the encoding using the setEncoding method.
		/// 
		/// If the system identifier is a URL, it must be fully resolved (it may not be a relative URL).

	const XMLString& getPublicId() const;
		/// Get the public identifier for this input source.
		
	const XMLString& getSystemId() const;
		/// Get the system identifier for this input source.

	void setByteStream(XMLByteInputStream& istr);
		/// Set the byte stream for this input source.
		/// The SAX parser will ignore this if there is also a character stream specified, but it 
		/// will use a byte stream in preference to opening a URI connection itself.
		
	XMLByteInputStream* getByteStream() const;
		/// Get the byte stream for this input source.

	void setCharacterStream(XMLCharInputStream& istr);
		/// Set the character stream for this input source.

	XMLCharInputStream* getCharacterStream() const;
		/// Get the character stream for this input source.

	void setEncoding(const XMLString& encoding);
		/// Set the character encoding, if known.
		/// The encoding must be a string acceptable for an XML encoding declaration 
		/// (see section 4.3.3 of the XML 1.0 recommendation).
		
	const XMLString& getEncoding() const;
		/// Get the character encoding for a byte stream or URI.

private:
	XMLString _publicId;
	XMLString _systemId;
	XMLString _encoding;
	XMLByteInputStream* _bistr;
	XMLCharInputStream* _cistr;
};


//
// inlines
//
inline const XMLString& InputSource::getPublicId() const
{
	return _publicId;
}


inline const XMLString& InputSource::getSystemId() const
{
	return _systemId;
}


inline const XMLString& InputSource::getEncoding() const
{
	return _encoding;
}


inline XMLByteInputStream* InputSource::getByteStream() const
{
	return _bistr;
}


inline XMLCharInputStream* InputSource::getCharacterStream() const
{
	return _cistr;
}


} } // namespace Poco::XML


#endif // SAX_InputSource_INCLUDED
