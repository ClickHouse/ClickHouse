#include <DB/Common/hex.h>
#include <DB/Common/StringUtils.h>
#include <DB/Common/UrlUtils.h>

std::string decodeUrl(const StringView& url)
{
	const char* p = url.data();
	const char* st = url.data();
	const char* end = url.data() + url.size();
	std::string result;

	for (; p < end; ++p)
	{
		if (*p != '%' || end - p < 3)
			continue;

		unsigned char h = char2DigitTable[static_cast<unsigned char>(p[1])];
		unsigned char l = char2DigitTable[static_cast<unsigned char>(p[2])];

		if (h != 0xFF && l != 0xFF)
		{
			unsigned char digit = (h << 4) + l;

			if (digit < 127) {
				result.append(st, p - st + 1);
				result.back() = digit;
				st = p + 3;
			}
		}

		p += 2;
	}

	if (st == url.data())
		return std::string(url.data(), url.size());
	if (st < p)
		result.append(st, p - st);
	return result;
}

StringView getUrlScheme(const StringView& url)
{
	// scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
	const char* p = url.data();
	const char* end = url.data() + url.size();

	if (isAlphaASCII(*p))
	{
		for (++p; p < end; ++p)
		{
			if (!(isAlphaNumericASCII(*p) || *p == '+' || *p == '-' || *p == '.'))
			{
				break;
			}
		}

		return StringView(url.data(), p - url.data());
	}

	return StringView();
}


StringView getUrlHost(const StringView& url)
{
	StringView scheme = getUrlScheme(url);
	const char* p = url.data() + scheme.size();
	const char* end = url.data() + url.size();

	// Colon must follows after scheme.
	if (p == end || *p != ':')
		return StringView();
	// Authority component must starts with "//".
	if (end - p < 2 || (p[1] != '/' || p[2] != '/'))
		return StringView();
	else
		p += 3;

	const char* st = p;

	for (; p < end; ++p)
	{
		if (*p == '@')
		{
			st = p + 1;
		}
		else if (*p == ':' || *p == '/' || *p == '?' || *p == '#')
		{
			break;
		}
	}

	return (p == st) ? StringView() : StringView(st, p - st);
}
