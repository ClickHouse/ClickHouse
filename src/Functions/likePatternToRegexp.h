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
        switch (*pos)
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
                res += *pos;
                ++pos;
                break;
            case '%':
                ++pos;
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
                ++pos;
                res += ".";
                break;
            case '\\':
                ++pos;
                if (pos == end)
                {
                    throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of LIKE pattern");
                }
                switch (*pos)
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
                    case '\\':
                        res += '\\';
                        res += *pos;
                        ++pos;
                        break;
                    default:
                        res += *pos;
                        break;
                }
                break;
            default:
                res += *pos;
                ++pos;
                break;
        }
    }

    res += '$';
    return res;
}

}
