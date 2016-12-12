#include <DB/IO/ReadHelpers.h>
#include <DB/Common/StringUtils.h>
#include <DB/Common/escapeForFileName.h>

namespace DB
{

std::string escapeForFileName(const std::string & s)
{
	std::string res;
	const char * pos = s.data();
	const char * end = pos + s.size();

	static const char * hex = "0123456789ABCDEF";

	while (pos != end)
	{
		char c = *pos;

		if (isWordCharASCII(c))
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

}
