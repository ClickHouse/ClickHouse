#pragma once

#include "FunctionsURL.h"
#include <common/find_symbols.h>


namespace DB
{

struct ExtractNetloc
{
    static size_t getReserveLengthForElement() { return 10; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = size;

        Pos pos = data;
        Pos end = pos + size;

        pos = find_first_symbols<'/'>(pos, end);
        if (end == pos)
            return;

        /// Note that strings are zero-terminated.
        bool has_subsequent_slash = pos[1] == '/';
        if (!has_subsequent_slash)
            return;
        res_data = pos + 2;
        res_size = end - res_data;

        /// Search for next slash.
        pos = find_first_symbols<'/', '?'>(pos + 2, end);
        if (end == pos)
            return;
        res_size = pos - res_data;
    }
};

}

