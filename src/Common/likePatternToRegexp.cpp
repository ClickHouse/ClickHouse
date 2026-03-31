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

String similarToPatternToRegexp(std::string_view pattern)
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
        /// SIMILAR TO's metachacraters consist of LIKE's and a subset of re2's:
        /// - LIKE's: %_         --> Convert to .* or . in re2
        /// - re2's: |*+?[](){}  --> Keep in re2
        /// - Exclude re2's: ^$. --> Quote in re2
        switch (*pos)
        {
            /// Quote characters which have a special meaning in re2
            case '^':
            case '$':
            case '.':
                res += '\\';
                res += *pos;
                break;
            /// Convert LIKE's metacharacters to re2's
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
                    /// Unquote LIKE metacharacters %, _ and \ as literals for re2:
                    case '%':
                    case '_':
                        res += pos[1];
                        ++pos;
                        break;
                    /// Quote blackslash
                    case '\\':
                        res += "\\\\";
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

}
