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

String likePatternWithCustomEscapeToLikePattern(std::string_view pattern, char escape_char)
{
    String res;
    res.reserve(pattern.size());

    const char * pos = pattern.data();
    const char * const end = pattern.data() + pattern.size();

    while (pos < end)
    {
        if (*pos == escape_char)
        {
            ++pos;
            if (pos == end)
                throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of LIKE pattern '{}'", pattern);

            if (*pos == '%' || *pos == '_')
            {
                /// escape_char + wildcard → \wildcard
                res += '\\';
                res += *pos;
            }
            else if (*pos == escape_char)
            {
                /// escape_char + escape_char → literal escape_char
                /// If escape_char is backslash, emit \\ (standard LIKE escape for literal backslash).
                /// Otherwise, just emit the escape_char as a regular character.
                if (escape_char == '\\')
                    res += "\\\\";
                else
                    res += *pos;
            }
            else
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE,
                    "Invalid escape sequence '{}{}' in LIKE pattern '{}': "
                    "the escape character must be followed by '%', '_', or the escape character itself",
                    escape_char, *pos, pattern);
            }
        }
        else if (*pos == '\\' && escape_char != '\\')
        {
            /// When a custom escape char is used, bare backslashes are literals
            res += "\\\\";
        }
        else
        {
            res += *pos;
        }
        ++pos;
    }

    return res;
}

}
