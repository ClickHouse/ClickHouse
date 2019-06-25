#pragma once

#include "protocol.h"
#include <common/find_symbols.h>
#include <cstring>


namespace DB
{

namespace
{

inline StringRef checkAndReturnHost(const Pos & pos, const Pos & dot_pos, const Pos & start_of_host)
{
    if (!dot_pos || start_of_host >= pos || pos - dot_pos == 1)
        return StringRef{};

    auto after_dot = *(dot_pos + 1);
    if (after_dot == ':' || after_dot == '/' || after_dot == '?' || after_dot == '#')
        return StringRef{};

    return StringRef(start_of_host, pos - start_of_host);
}

}

/// Extracts host from given url.
inline StringRef getURLHost(const char * data, size_t size)
{
    Pos pos = data;
    Pos end = data + size;

    Pos slash_pos = find_first_symbols<'/'>(pos, end);
    if (slash_pos < end - 1 && *(slash_pos + 1) == '/')
        pos = slash_pos + 2;
    else
        pos = data;

    if (pos != data)
    {
        StringRef scheme = getURLScheme(data, pos - data - 2);
        Pos scheme_end = data + scheme.size;
        if (scheme.size && (pos - scheme_end != 3 || *scheme_end != ':'))
            return StringRef{};
    }

    auto start_of_host = pos;
    Pos dot_pos = nullptr;
    for (; pos < end; ++pos)
    {
        switch (*pos)
        {
        case '.':
            dot_pos = pos;
            break;
        case ':': /// end symbols
        case '/':
        case '?':
        case '#':
            return checkAndReturnHost(pos, dot_pos, start_of_host);
        case '@': /// myemail@gmail.com
            start_of_host = pos + 1;
            break;
        case ' ': /// restricted symbols
        case '\t':
        case '<':
        case '>':
        case '%':
        case '{':
        case '}':
        case '|':
        case '\\':
        case '^':
        case '~':
        case '[':
        case ']':
        case ';':
        case '=':
        case '&':
            return StringRef{};
        }
    }

    return checkAndReturnHost(pos, dot_pos, start_of_host);
}

template <bool without_www>
struct ExtractDomain
{
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        StringRef host = getURLHost(data, size);

        if (host.size == 0)
        {
            res_data = data;
            res_size = 0;
        }
        else
        {
            if (without_www && host.size > 4 && !strncmp(host.data, "www.", 4))
                host = { host.data + 4, host.size - 4 };

            res_data = host.data;
            res_size = host.size;
        }
    }
};

}
