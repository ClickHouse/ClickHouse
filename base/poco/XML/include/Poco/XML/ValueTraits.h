//
// ValueTraits.h
//
// Library: XML
// Package: XML
// Module:  ValueTraits
//
// Definition of the ValueTraits templates.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Based on libstudxml (http://www.codesynthesis.com/projects/libstudxml/).
// Copyright (c) 2009-2013 Code Synthesis Tools CC.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_ValueTraits_INCLUDED
#define XML_ValueTraits_INCLUDED


#include "XMLStreamParserException.h"
#include <string>
#include <cstddef>
#include <iostream>
#include <sstream>


namespace Poco {
namespace XML {


class XMLStreamParser;
class XMLStreamSerializer;


template <typename T>
struct DefaultValueTraits
{
	static T
	parse(std::string, const XMLStreamParser&);

	static std::string
	serialize(const T&, const XMLStreamSerializer&);
};


template <>
struct XML_API DefaultValueTraits<bool>
{
	static bool
	parse(std::string, const XMLStreamParser&);

	static std::string serialize(bool v, const XMLStreamSerializer&)
	{
		return v ? "true" : "false";
	}
};


template <>
struct XML_API DefaultValueTraits<std::string>
{
	static std::string parse(std::string s, const XMLStreamParser&)
	{
		return s;
	}

	static std::string serialize(const std::string& v, const XMLStreamSerializer&)
	{
		return v;
	}
};


template <typename T>
struct ValueTraits: DefaultValueTraits<T>
{
};


template <typename T, std::size_t N>
struct ValueTraits<T[N]> : DefaultValueTraits<const T*>
{
};


template <typename T>
T DefaultValueTraits<T>::parse(std::string s, const XMLStreamParser& p)
{
	T r;
	std::istringstream is(s);
	if (!(is >> r && is.eof()))
		throw XMLStreamParserException(p, "invalid value '" + s + "'");
	return r;
}


} } // namespace Poco::XML


#endif // XML_ValueTraits_INCLUDED
