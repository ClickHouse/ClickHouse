//
// String.h
//
// Library: Foundation
// Package: Core
// Module:  String
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#include "Poco/JSONString.h"
#include "Poco/UTF8String.h"
#include <ostream>


namespace {


template<typename T, typename S>
struct WriteFunc
{
	typedef T& (T::*Type)(const char* s, S n);
};



template<typename T, typename S>
void writeString(const std::string &value, T& obj, typename WriteFunc<T, S>::Type write, int options)
{
	bool wrap = ((options & Poco::JSON_WRAP_STRINGS) != 0);
	bool escapeAllUnicode = ((options & Poco::JSON_ESCAPE_UNICODE) != 0);

	if (value.size() == 0)
	{
		if(wrap) (obj.*write)("\"\"", 2);
		return;
	}

	if(wrap) (obj.*write)("\"", 1);
	if(escapeAllUnicode)
	{
		std::string str = Poco::UTF8::escape(value.begin(), value.end(), true);
		(obj.*write)(str.c_str(), str.size());
	}
	else
	{
		for(std::string::const_iterator it = value.begin(), end = value.end(); it != end; ++it)
		{
			// Forward slash isn't strictly required by JSON spec, but some parsers expect it
			if((*it >= 0 && *it <= 31) || (*it == '"') || (*it == '\\') || (*it == '/'))
			{
				std::string str = Poco::UTF8::escape(it, it + 1, true);
				(obj.*write)(str.c_str(), str.size());
			}else (obj.*write)(&(*it), 1);
		}
	}
	if(wrap) (obj.*write)("\"", 1);
};


}


namespace Poco {


void toJSON(const std::string& value, std::ostream& out, bool wrap)
{
	int options = (wrap ? Poco::JSON_WRAP_STRINGS : 0);
	writeString<std::ostream, std::streamsize>(value, out, &std::ostream::write, options);
}


std::string toJSON(const std::string& value, bool wrap)
{
	int options = (wrap ? Poco::JSON_WRAP_STRINGS : 0);
	std::string ret;
	writeString<std::string,
				std::string::size_type>(value, ret, &std::string::append, options);
	return ret;
}


void toJSON(const std::string& value, std::ostream& out, int options)
{
	writeString<std::ostream, std::streamsize>(value, out, &std::ostream::write, options);
}


std::string toJSON(const std::string& value, int options)
{
	std::string ret;
	writeString<std::string, std::string::size_type>(value, ret, &std::string::append, options);
	return ret;
}


} // namespace Poco
