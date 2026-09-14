//
// XMLString.cpp
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


#include <cstdlib>
#include <new>
#if defined(XML_UNICODE_WCHAR_T)
#include <stdlib.h>
#endif


namespace Poco {
namespace XML {


__attribute__((weak)) void* allocateNoDump(std::size_t bytes)
{
	if (void* ptr = std::malloc(bytes))
		return ptr;
	throw std::bad_alloc();
}


__attribute__((weak)) void deallocateNoDump(void* ptr) noexcept
{
	std::free(ptr);
}


#if defined(XML_UNICODE_WCHAR_T)


std::string fromXMLString(const XMLString& str)
{
	std::string result;
	result.reserve(str.size());
	
	for (auto xc: str)
	{
		char c;
		wctomb(&c, xc);
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
