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

inline char unhex(char c)
{
	switch (c)
	{
		case '0' ... '9':
			return c - '0';
		case 'A' ... 'F':
			return c - 'A' + 10;
		default:
			return 0;
	}
}

inline std::string unescapeForFileName(const std::string & s)
{
	std::string res;
	const char * pos = s.data();
	const char * end = pos + s.size();
	
	while (pos != end)
	{
		if (*pos != '%')
			res += *pos;
		else
		{
			/// пропустим '%'
			if (++pos == end) break;
			
			char val = unhex(*pos) * 16;
			
			if (++pos == end) break;
			
			val += unhex(*pos);
			
			res += val;
		}
		
		++pos;
	}
	return res;
}
