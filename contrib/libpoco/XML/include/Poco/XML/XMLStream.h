//
// XMLStream.h
//
// $Id: //poco/1.4/XML/include/Poco/XML/XMLStream.h#1 $
//
// Library: XML
// Package: XML
// Module:  XMLStream
//
// Definition of the XMLByteInputStream and XMLCharInputStream classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XMLStream_INCLUDED
#define XML_XMLStream_INCLUDED


#include "Poco/XML/XML.h"
#include <istream>
#include <ostream>


namespace Poco {
namespace XML {


// The byte input stream is always a narrow stream.
typedef std::istream XMLByteInputStream;
typedef std::ostream XMLByteOutputStream;


//
// The XML parser uses the stream classes provided by the C++
// standard library (based on the basic_stream<> template).
// In Unicode mode, a wide stream is used.
// To turn on Unicode mode, #define XML_UNICODE and
// XML_UNICODE_WCHAR_T when compiling the library.
//
// XML_UNICODE  XML_UNICODE_WCHAR_T  XMLCharInputStream  XMLCharOutputStream
// -------------------------------------------------------------------------
//     N                 N           std::istream        std::ostream
//     N                 Y           std::wistream       std::wostream
//     Y                 Y           std::wistream       std::wostream
//     Y                 N           <not supported>
//
#if defined(XML_UNICODE_WCHAR_T)

	// Unicode - use wide streams
	typedef std::wistream XMLCharInputStream;
	typedef std::wostream XMLCharOutputStream;

#elif defined(XML_UNICODE)

	// not supported - leave XMLString undefined

#else

	// Characters are UTF-8 encoded
	typedef std::istream XMLCharInputStream;
	typedef std::ostream XMLCharOutputStream;

#endif


} } // namespace Poco::XML


#endif // XML_XMLStream_INCLUDED
