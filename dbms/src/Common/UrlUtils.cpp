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
