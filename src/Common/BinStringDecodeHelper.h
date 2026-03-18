#pragma once

#include <base/hex.h>

namespace DB
{

static void inline hexStringDecode(const char * pos, const char * end, char *& out, size_t word_size)
{
    if ((end - pos) & 1)
    {
        *out = unhex(*pos);
        ++out;
        ++pos;
    }
    while (pos < end)
    {
        *out = unhex2(pos);
        pos += word_size;
        ++out;
    }
    *out = '\0';
    ++out;
}

static void inline binStringDecode(const char * pos, const char * end, char *& out, size_t word_size)
{
    if (pos == end)
    {
        *out = '\0';
        ++out;
        return;
    }

    UInt8 left = 0;

    /// end - pos is the length of input.
    /// (length & 7) to make remain bits length mod 8 is zero to split.
    /// e.g. the length is 9 and the input is "101000001",
    /// first left_cnt is 1, left is 0, right shift, pos is 1, left = 1
    /// then, left_cnt is 0, remain input is '01000001'.
    for (UInt8 left_cnt = (end - pos) & 7; left_cnt > 0; --left_cnt)
    {
        left = left << 1;
        if (*pos != '0')
            left += 1;
        ++pos;
    }

    if (left != 0 || end - pos == 0)
    {
        *out = left;
        ++out;
    }

    chassert((end - pos) % word_size == 0);

    while (end - pos != 0)
    {
        UInt8 c = 0;
        for (UInt8 i = 0; i < 8; ++i)
        {
            c = c << 1;
            if (*pos != '0')
                c += 1;
            ++pos;
        }
        *out = c;
        ++out;
    }

    *out = '\0';
    ++out;
}

}
