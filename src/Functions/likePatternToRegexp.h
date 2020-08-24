#pragma once

#include <Core/Types.h>

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
                ++pos;
                if (pos == end)
                    res += "\\\\";
                else
                {
                    if (*pos == '%' || *pos == '_')
                        res += *pos;
                    else
                    {
                        res += '\\';
                        res += *pos;
                    }
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
