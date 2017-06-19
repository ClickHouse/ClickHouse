#pragma once

#include <cstring>
#include <cmath>
#include <string>
#include <Core/Types.h>

#define UNICODE_BAR_CHAR_SIZE (strlen("█"))


/** Allows you to draw a unicode-art bar whose width is displayed with a resolution of 1/8 character.
  */


namespace UnicodeBar
{
    using DB::Int64;

    inline double getWidth(Int64 x, Int64 min, Int64 max, double max_width)
    {
        if (x <= min)
            return 0;

        if (x >= max)
            return max_width;

        return (x - min) * max_width / (max - min);
    }

    inline size_t getWidthInBytes(double width)
    {
        return ceil(width - 1.0 / 8) * UNICODE_BAR_CHAR_SIZE;
    }

    /// In `dst` there must be a space for barWidthInBytes(width) characters and a trailing zero.
    inline void render(double width, char * dst)
    {
        size_t floor_width = floor(width);

        for (size_t i = 0; i < floor_width; ++i)
        {
            memcpy(dst, "█", UNICODE_BAR_CHAR_SIZE);
            dst += UNICODE_BAR_CHAR_SIZE;
        }

        size_t remainder = floor((width - floor_width) * 8);

        if (remainder)
        {
            memcpy(dst, &"▏▎▍▌▋▋▊▉"[(remainder - 1) * UNICODE_BAR_CHAR_SIZE], UNICODE_BAR_CHAR_SIZE);
            dst += UNICODE_BAR_CHAR_SIZE;
        }

        *dst = 0;
    }

    inline std::string render(double width)
    {
        std::string res(getWidthInBytes(width), '\0');
        render(width, &res[0]);
        return res;
    }
}
