#include <cstring>
#include <cmath>
#include <string>
#include <common/types.h>
#include <common/arithmeticOverflow.h>
#include <Common/Exception.h>
#include <Common/UnicodeBar.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int PARAMETER_OUT_OF_BOUND;
    }
}


namespace UnicodeBar
{
    double getWidth(Int64 x, Int64 min, Int64 max, double max_width)
    {
        if (x <= min)
            return 0;

        if (x >= max)
            return max_width;

        /// The case when max - min overflows
        Int64 max_difference;
        if (common::subOverflow(max, min, max_difference))
            throw DB::Exception(DB::ErrorCodes::PARAMETER_OUT_OF_BOUND, "The arguments to render unicode bar will lead to arithmetic overflow");

        return (x - min) * max_width / max_difference;
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

