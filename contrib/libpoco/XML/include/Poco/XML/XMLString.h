//
// XMLString.h
//
// $Id: //poco/1.4/XML/include/Poco/XML/XMLString.h#1 $
//
// Library: XML
// Package: XML
// Module:  XMLString
//
// Definition of the XMLString class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XMLString_INCLUDED
#define XML_XMLString_INCLUDED


#include "Poco/XML/XML.h"


namespace Poco {
namespace XML {


//
// The XML parser uses the string classes provided by the C++
// standard library (based on the basic_string<> template).
// In Unicode mode, a std::wstring is used, otherwise
// a std::string is used.
// To turn on Unicode mode, #define XML_UNICODE and
// XML_UNICODE_WCHAR_T when compiling the library.
//
// XML_UNICODE  XML_UNICODE_WCHAR_T  XMLChar    XMLString
// --------------------------------------------------------------
//     N                 N           char       std::string
//     N                 Y           wchar_t    std::wstring
//     Y                 Y           wchar_t    std::wstring
//     Y                 N           <not supported>
//
#if defined(XML_UNICODE_WCHAR_T)

	// Unicode - use wchar_t
	typedef wchar_t      XMLChar;
	typedef std::wstring XMLString;

	std::string fromXMLString(const XMLString& str);
		/// Converts an XMLString into an UTF-8 encoded
		/// string.
		
	XMLString toXMLString(const std::string& str);
		/// Converts an UTF-8 encoded string into an
		/// XMLString
		
	#define XML_LIT(lit) L##lit

#elif defined(XML_UNICODE)

	// not supported - leave XMLString undefined

#else

	// Characters are UTF-8 encoded
	typedef char        XMLChar;
	typedef std::string XMLString;

	inline const std::string& fromXMLString(const XMLString& str)
	{
		return str;
	}

	inline const XMLString& toXMLString(const std::string& str)
	{
		return str;
	}
	
	#define XML_LIT(lit) lit

#endif


} } // namespace Poco::XML


#endif // XML_XMLString_INCLUDED
