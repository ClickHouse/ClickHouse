#pragma once

#include "protocol.h"
#include <common/find_symbols.h>
#include <cstring>
#include <Common/StringUtils/StringUtils.h>
#include <Common/StringSearcher.h>

namespace DB
{

namespace
{

const ASCIICaseSensitiveStringSearcher SCHEME_SEARCHER{"://", 3};

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

    if (*pos == '/' && *(pos + 1) == '/')
    {
        pos += 2;
    }
    else
    {
        size_t max_scheme_size = std::min(size, 16UL);
        Pos scheme_end = reinterpret_cast<Pos>(SCHEME_SEARCHER.search(reinterpret_cast<const UInt8 *>(data), max_scheme_size));
        if (scheme_end != data + max_scheme_size)
            pos = scheme_end + 3;
    }

    Pos dot_pos = nullptr;
    auto start_of_host = pos;
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
        case ' ': /// restricted symbols in whole URL
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
