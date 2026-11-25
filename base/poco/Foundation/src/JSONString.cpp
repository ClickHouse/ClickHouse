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


/// `write` is a callable rather than a pointer to a member function: Clang CFI (`cfi-mfcall`) rejected the call
/// through `&std::ostream::write`.
template<typename Write>
void writeString(const std::string &value, Write && write, int options)
{
	bool wrap = ((options & Poco::JSON_WRAP_STRINGS) != 0);
	bool escapeAllUnicode = ((options & Poco::JSON_ESCAPE_UNICODE) != 0);

	if (value.size() == 0)
	{
		if(wrap) write("\"\"", 2);
		return;
	}

	if(wrap) write("\"", 1);
	if(escapeAllUnicode)
	{
		std::string str = Poco::UTF8::escape(value.begin(), value.end(), true);
		write(str.c_str(), str.size());
	}
	else
	{
		for(std::string::const_iterator it = value.begin(), end = value.end(); it != end; ++it)
		{
			// Forward slash isn't strictly required by JSON spec, but some parsers expect it
			if((*it >= 0 && *it <= 31) || (*it == '"') || (*it == '\\') || (*it == '/'))
			{
				std::string str = Poco::UTF8::escape(it, it + 1, true);
				write(str.c_str(), str.size());
			}else write(&(*it), 1);
		}
	}
	if(wrap) write("\"", 1);
};


}


namespace Poco {


void toJSON(const std::string& value, std::ostream& out, bool wrap)
{
	int options = (wrap ? Poco::JSON_WRAP_STRINGS : 0);
	writeString(value, [&out](const char * s, size_t n) { out.write(s, static_cast<std::streamsize>(n)); }, options);
}


std::string toJSON(const std::string& value, bool wrap)
{
	int options = (wrap ? Poco::JSON_WRAP_STRINGS : 0);
	std::string ret;
	writeString(value, [&ret](const char * s, size_t n) { ret.append(s, n); }, options);
	return ret;
}


void toJSON(const std::string& value, std::ostream& out, int options)
{
	writeString(value, [&out](const char * s, size_t n) { out.write(s, static_cast<std::streamsize>(n)); }, options);
}


std::string toJSON(const std::string& value, int options)
{
	std::string ret;
	writeString(value, [&ret](const char * s, size_t n) { ret.append(s, n); }, options);
	return ret;
}


} // namespace Poco
