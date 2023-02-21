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

    char char_slot;
    if (pos < end && *pos == '%')
    {
        while (++pos < end && *pos == '%')
        {
        }
    }
    else
    {
        res = "^";
    }
    while (pos != end)
    {
        char_slot = *(pos++);
        switch (char_slot)
        {
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
                res += char_slot;
                break;
            case '%':
                if (pos != end)
                {
                    res += ".*";
                }
                else
                {
                    return res;
                }
                break;
            case '_':
                res += ".";
                break;
            case '\\':
                if (pos == end)
                {
                    throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of LIKE pattern");
                }
                else
                {
                    res += '\\';
                    res += *(pos++);
                    break;
                }
            default:
                res += char_slot;
                break;
        }
    }

    res += '$';
    return res;
}

}
