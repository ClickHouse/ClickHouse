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

std::string unescapeForFileName(const std::string & s)
{
	std::string res;
	const char * pos = s.data();
	const char * end = pos + s.size();
	
	while (pos != end)
	{
		char c = *pos;
		
		if (c != '%')
			res += c;
		else
		{
			/// пропустим '%'
			if (++pos == end) break;
			c = *pos;
			
			char val = 0;
			
			switch (c)
			{
				case '0' ... '9':
					val = (c - '0') * 16;
					break;
				case 'A' ... 'F':
					val = (c - 'A' + 10) * 16;
					break;
			};
			
			if (++pos == end) break;
			c = *pos;
			
			switch (c)
			{
				case '0' ... '9':
					val += (c - '0');
					break;
				case 'A' ... 'F':
					val += c - 'A' + 10;
					break;
			};
			res += val;
		}
		
		++pos;
	}
	return res;
}
