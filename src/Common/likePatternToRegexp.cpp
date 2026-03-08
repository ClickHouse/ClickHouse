#include <Common/likePatternToRegexp.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
}

String likePatternToRegexp(std::string_view pattern)
{
    String res;
    res.reserve(pattern.size() * 2);

    const char * pos = pattern.data();
    const char * const end = pattern.data() + pattern.size();

    if (pos < end && *pos == '%')
        /// Eat leading %
        while (++pos < end)
        {
            if (*pos != '%')
                break;
        }
    else
        res = "^";

    while (pos < end)
    {
        switch (*pos)
        {
            /// Quote characters which have a special meaning in re2
            case '^':
            case '$':
            case '.':
            case '[':
            case '|':
            case '(':
            case ')':
            case '?':
            case '*':
            case '+':
            case '{':
                res += '\\';
                res += *pos;
                break;
            case '%':
                if (pos + 1 != end)
                    res += ".*";
                else
                    return res;
                break;
            case '_':
                res += ".";
                break;
            case '\\':
                if (pos + 1 == end)
                    throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of LIKE pattern '{}'", pattern);
                switch (pos[1])
                {
                    /// Interpret quoted LIKE metacharacters %, _ and \ as literals:
                    case '%':
                    case '_':
                        res += pos[1];
                        ++pos;
                        break;
                    case '\\':
                        res += "\\\\"; /// backslash has a special meaning in re2 --> quote it
                        ++pos;
                        break;
                    /// Unknown escape sequence treated literally: as backslash (which must be quoted in re2) + the following character
                    default:
                        res += "\\\\";
                        break;
                }
                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    res += '$';
    return res;
}

bool likePatternIsSubstring(std::string_view pattern, String & res)
{
    /// TODO: ignore multiple leading or trailing %
    if (pattern.size() < 2 || !pattern.starts_with('%') || !pattern.ends_with('%'))
        return false;

    res.clear();
    res.reserve(pattern.size() - 2);

    const char * pos = pattern.data() + 1;
    const char * const end = pattern.data() + pattern.size() - 1;

    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
                return false;
            case '\\':
                ++pos;
                if (pos == end)
                    /// pattern ends with \% --> trailing % is to be taken literally and pattern doesn't qualify for substring search
                    return false;

                switch (*pos)
                {
                    /// Known LIKE escape sequences:
                    case '%':
                    case '_':
                    case '\\':
                        res += *pos;
                        break;
                    /// For all other escape sequences, the backslash loses its special meaning
                    default:
                        res += '\\';
                        res += *pos;
                        break;
                }

                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    return true;
}

}
