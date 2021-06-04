#pragma once

#include <common/types.h>

namespace DB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
inline String likePatternToRegexp(const String & pattern)
{
    String res;
    res.reserve(pattern.size() * 2);
    const char * pos = pattern.data();
    const char * end = pos + pattern.size();

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
                /// Known escape sequences.
                if (pos + 1 != end && (pos[1] == '%' || pos[1] == '_'))
                {
                    res += pos[1];
                    ++pos;
                }
                else if (pos + 1 != end && pos[1] == '\\')
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
