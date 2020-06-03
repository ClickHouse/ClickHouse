#pragma once

#include "FunctionsURL.h"
#include <common/find_symbols.h>


namespace DB
{

struct ExtractNetloc
{
    /// We use the same as domain function
    static size_t getReserveLengthForElement() { return 15; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = size;

        Pos pos = data;
        Pos end = pos + size;

        pos = find_first_symbols<'/'>(pos, end);
        if (end == pos)
            return;

        /// Strings are zero-terminated.
        bool has_subsequent_slash = pos[1] == '/';
        if (!has_subsequent_slash)
            return;
        /// Search for next slash or question mark
        /// Note than currently the netloc function doesn't support
        /// if we have a question mark as an username or password.
        /// This choice has been made for not hurting performance with still taking into
        /// acount URL without a slash in the end of a query, with a query string.
        res_data = pos + 2;
        res_size = find_first_symbols<'/', '?'>(pos + 2, end) - res_data;
    }
};

}

