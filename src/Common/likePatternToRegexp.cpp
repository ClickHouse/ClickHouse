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

    bool in_bracket = false;
    while (pos < end)
    {
        /// SIMILAR TO's metacharacters consist of LIKE's and a subset of re2's:
        ///   - LIKE's: %_         --> Convert to .* or . in re2
        ///   - re2's: |*+?[](){}  --> Keep in re2
        ///   - Exclude re2's: ^$. --> Quote in re2
        /// \ always starts an escape sequence
        /// Inside a bracket expression, only ^, \, -, ] are special; all others are literal
        switch (*pos)
        {
            /// Keep unescaped brackets. Remember in bracket or not.
            case '[':
                in_bracket = true;
                res += *pos;
                break;
            case ']':
                in_bracket = false;
                res += *pos;
                break;
            /// Quote characters which have a special meaning in re2. Don't quote when in bracket.
            case '^':
            case '$':
            case '.':
                if (! in_bracket)
                    res += '\\';
                res += *pos;
                break;
            /// Convert LIKE's metacharacters to re2's. Don't convert when in bracket.
            case '%':
                if (! in_bracket)
                {
                    if (pos + 1 != end)
                        res += ".*";
                    else
                        return res;
                }
                else
                    res += *pos;
                break;
            case '_':
                if (! in_bracket)
                    res += ".";
                else
                    res += *pos;
                break;
            case '\\':
                if (pos + 1 == end)
                    throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of SIMILAR TO pattern '{}'", pattern);
                switch (pos[1])
                {
                    /// Unquote LIKE metacharacters %, _ and \ as literals for re2:
                    case '%':
                    case '_':
                        res += pos[1];
                        ++pos;
                        break;
                    /// Keep escaped SIMILAR TO excluding LIKE metacharacters for re2:
#define CASES(c) case c:
                    SIMILAR_TO_EXCLUDING_LIKE_METACHARS(CASES)
#undef CASES
                        res += '\\';
                        res += pos[1];
                        ++pos;
                        break;
                    /// Quote backslash
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
