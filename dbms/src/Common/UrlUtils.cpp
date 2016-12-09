#include <DB/Common/StringUtils.h>
#include <DB/Common/UrlUtils.h>

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
