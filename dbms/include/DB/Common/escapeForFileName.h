#pragma once

#include <string>


inline std::string escapeForFileName(const std::string & s)
{
	std::string res;
	const char * pos = s.data();
	const char * end = pos + s.size();

	static const char * hex = "0123456789ABCDEF";

	while (pos != end)
	{
		char c = *pos;

		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c == '_'))
			res += c;
		else
		{
			res += '%';
			res += hex[c / 16];
			res += hex[c % 16];
		}

		++pos;
	}

	return res;
}
