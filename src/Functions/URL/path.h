#pragma once

#include <common/find_symbols.h>
#include <Functions/URL/FunctionsURL.h>


namespace DB
{

template <bool with_query_string>
struct ExtractPath
{
    static size_t getReserveLengthForElement() { return 25; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        Pos pos = data;
        Pos end = pos + size;

        /// We support URLs with and without schema:
        /// 1. http://host/path
        /// 2. host/path
        /// We search for first slash and if there is subsequent slash, then skip and repeat search for the next slash.

        pos = find_first_symbols<'/'>(pos, end);
        if (end == pos)
            return;

        /// Note that strings are zero-terminated.
        bool has_subsequent_slash = pos[1] == '/';
        if (has_subsequent_slash)
        {
            /// Search for next slash.
            pos = find_first_symbols<'/'>(pos + 2, end);
            if (end == pos)
                return;
        }

        res_data = pos;

        if constexpr (with_query_string)
        {
            res_size = end - res_data;
        }
        else
        {
            Pos query_string_or_fragment = find_first_symbols<'?', '#'>(pos, end);
            res_size = query_string_or_fragment - res_data;
        }
    }
};

}
