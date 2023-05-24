#pragma once

#include <Common/Exception.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
}

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
inline String likePatternToRegexp(std::string_view pattern)
{
    String res;
    res.reserve(pattern.size() * 2);

    const char * pos = pattern.data();
    const char * const end = pattern.begin() + pattern.size();

    if (pos < end && *pos == '%')
        ++pos;
    else
        res = "^";

    while (pos < end)
    {
        switch (*pos)
        {
            case '^': case '$': case '.': case '[': case '|': case '(': case ')': case '?': case '*': case '+': case '{':
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
                    throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of LIKE pattern");
                /// Known escape sequences.
                if (pos[1] == '%' || pos[1] == '_')
                {
                    res += pos[1];
                    ++pos;
                }
                else if (pos[1] == '\\')
                {
                    res += "\\\\";
                    ++pos;
                }
                else
                {
                    /// Unknown escape sequence treated literally: as backslash and the following character.
                    res += "\\\\";
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
