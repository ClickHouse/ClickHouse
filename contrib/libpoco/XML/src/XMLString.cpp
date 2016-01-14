//
// XMLString.cpp
//
// $Id: //poco/1.4/XML/src/XMLString.cpp#1 $
//
// Library: XML
// Package: XML
// Module:  XMLString
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/XMLString.h"


#if defined(XML_UNICODE_WCHAR_T)
#include <stdlib.h>
#endif


namespace Poco {
namespace XML {


#if defined(XML_UNICODE_WCHAR_T)


std::string fromXMLString(const XMLString& str)
{
	std::string result;
	result.reserve(str.size());
	
	for (XMLString::const_iterator it = str.begin(); it != str.end(); ++it)
	{
		char c;
		wctomb(&c, *it);
		result += c;
	}
	return result;
}


XMLString toXMLString(const std::string& str)
{
	XMLString result;
	result.reserve(str.size());
	
	for (std::string::const_iterator it = str.begin(); it != str.end();)
	{
		wchar_t c;
		int n = mbtowc(&c, &*it, MB_CUR_MAX);
		result += c;
		it += (n > 0 ? n : 1);
	}
	return result;
}


#endif // XML_UNICODE_WCHAR_T


} } // namespace Poco::XML
