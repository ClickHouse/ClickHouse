#include <cstring>
#include <cmath>
#include <string>
#include <common/types.h>
#include <common/arithmeticOverflow.h>
#include <Common/Exception.h>
#include <Common/UnicodeBar.h>
#include <Common/NaNUtils.h>

#include <iostream>


namespace UnicodeBar
{
    double getWidth(double x, double min, double max, double max_width)
    {
        if (isNaN(x) || isNaN(min) || isNaN(max))
            return 0;

        if (x <= min)
            return 0;

        if (x >= max)
            return max_width;

        return (x - min) / (max - min) * max_width;
    }

    size_t getWidthInBytes(double width)
    {
        return ceil(width - 1.0 / 8) * UNICODE_BAR_CHAR_SIZE;
    }

    void render(double width, char * dst)
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

    std::string render(double width)
    {
        std::string res(getWidthInBytes(width), '\0');
        render(width, res.data());
        return res;
    }
}

