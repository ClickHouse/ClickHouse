#pragma once

#include "protocol.h"
#include <common/find_symbols.h>
#include <cstring>


namespace DB
{

/// Extracts host from given url.
inline StringRef getURLHost(const char * data, size_t size)
{
    Pos pos = data;
    Pos end = data + size;

    if (*(end - 1) == '.')
        return StringRef{};

    StringRef scheme = getURLScheme(data, size);
    if (scheme.size != 0)
    {
        Pos scheme_end = data + scheme.size;
        pos = scheme_end + 1;
        if (*scheme_end != ':' || *pos != '/')
            return StringRef{};
    }

    if (end - pos > 2 && *pos == '/' && *(pos + 1) == '/')
        pos += 2;

    auto start_of_host = pos;
    Pos dot_pos = nullptr;
    bool exit_loop = false;
    for (; pos < end && !exit_loop; ++pos)
    {
        switch(*pos)
        {
        case '.':
            dot_pos = pos;
            break;
        case ':': /// end symbols
        case '/':
        case '?':
        case '#':
            exit_loop = true;
            break;
        case '@': /// myemail@gmail.com
            start_of_host = pos;
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

    if (!dot_pos || start_of_host >= pos)
        return StringRef{};

    /// if end found immediately after dot
    char after_dot = *(dot_pos + 1);
    if (after_dot == ':' || after_dot == '/' || after_dot == '?' || after_dot == '#')
        return StringRef{};


    return StringRef(start_of_host, pos - start_of_host);
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
