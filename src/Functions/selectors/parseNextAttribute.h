#pragma once

#include "Types.h"

#include <base/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>

#include <string_view>

namespace DB
{

inline const char * parseNextAttribute(const char * begin, const char * end, Attribute & result)
{
    begin = find_first_not_symbols<' ', '\t', '\n', '\r', '\f', '\v'>(begin, end);
    if (begin == end)
        return end;
    if (*begin == '>')
        return begin;

    const char * key_end = find_first_symbols<'>', '=', ' ', '\t', '\n', '\r', '\f', '\v'>(begin, end);
    if (key_end == end)
        return end;

    result.key = std::string_view(begin, key_end - begin);

    begin = key_end;

    const char * eq_pos = find_first_symbols<'='>(begin, end);

    if (eq_pos + 1 >= end)
        return end;

    begin = eq_pos + 1;

    begin = find_first_not_symbols<' ', '\t', '\n', '\r', '\f', '\v'>(begin, end);
    if (begin == end || *begin == '>')
        return begin;

    const char * value_end = end;
    bool has_quotes = false;
    if (*begin == '\'')
    {
        ++begin;
        value_end = find_first_symbols<'\''>(begin, value_end);
        has_quotes = true;
    }
    else if (*begin == '"')
    {
        ++begin;
        value_end = find_first_symbols<'"'>(begin, value_end);
        has_quotes = true;
    }
    else
    {
        value_end = find_first_symbols<'>', ' ', '\t', '\n', '\r', '\f', '\v'>(begin, value_end);
    }

    if (value_end == end) {
        return end;
    }

    result.value = std::string_view(begin, value_end - begin);

    return has_quotes ? value_end + 1 : value_end;
}

}
