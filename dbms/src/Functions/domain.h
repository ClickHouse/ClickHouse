#pragma once

#include <Functions/protocol.h>
#include <common/find_symbols.h>
#include <cstring>


namespace DB
{

/// Extracts host from given url.
inline StringRef getURLHost(const char * data, size_t size)
{
    Pos pos = data;
    Pos end = data + size;

    if (end == (pos = find_first_symbols<'/'>(pos, end)))
        return {};

    if (pos != data)
    {
        StringRef scheme = getURLScheme(data, size);
        Pos scheme_end = data + scheme.size;

        // Colon must follows after scheme.
        if (pos - scheme_end != 1 || *scheme_end != ':')
            return {};
    }

    if (end - pos < 2 || *(pos) != '/' || *(pos + 1) != '/')
        return {};
    pos += 2;

    const char * start_of_host = pos;
    for (; pos < end; ++pos)
    {
        if (*pos == '@')
            start_of_host = pos + 1;
        else if (*pos == ':' || *pos == '/' || *pos == '?' || *pos == '#')
            break;
    }

    return (pos == start_of_host) ? StringRef{} : StringRef(start_of_host, pos - start_of_host);
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
