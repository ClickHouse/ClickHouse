#include <cstring>
#include <cmath>
#include <string>
#include <base/types.h>
#include <base/arithmeticOverflow.h>
#include <Common/Exception.h>
#include <Common/UnicodeBar.h>
#include <Common/NaNUtils.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }
}

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

    namespace
    {
        /// We use the following Unicode characters to draw the bar:
        /// U+2588 "█" Full block
        /// U+2589 "▉" Left seven eighths block
        /// U+258A "▊" Left three quarters block
        /// U+258B "▋" Left five eighths block
        /// U+258C "▌" Left half block
        /// U+258D "▍" Left three eighths block
        /// U+258E "▎" Left one quarter block
        /// U+258F "▏" Left one eighth block
        constexpr size_t GRADES_IN_FULL_BAR = 8;
        constexpr char FULL_BAR[] = "█";
        constexpr char FRACTIONAL_BARS[] = "▏▎▍▌▋▊▉";   /// 7 elements: 1/8, 2/8, 3/8, 4/8, 5/8, 6/8, 7/8
    }

    size_t getWidthInBytes(double width)
    {
        Int64 int_width = static_cast<Int64>(width * GRADES_IN_FULL_BAR);
        return (int_width / GRADES_IN_FULL_BAR) * UNICODE_BAR_CHAR_SIZE + (int_width % GRADES_IN_FULL_BAR ? UNICODE_BAR_CHAR_SIZE : 0);
    }

    static char* checkedCopy(const char * src, size_t src_size, char * dst, const char * dst_end)
    {
        if (dst + src_size > dst_end)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Not enough space in buffer for UnicodeBar::render, required: {}, got: {}",
                src_size, dst_end - dst);

        memcpy(dst, src, src_size);
        return dst + src_size;
    }

    void render(double width, char * dst, const char * dst_end)
    {
        Int64 int_width = static_cast<Int64>(width * GRADES_IN_FULL_BAR);
        size_t floor_width = (int_width / GRADES_IN_FULL_BAR);

        for (size_t i = 0; i < floor_width; ++i)
            dst = checkedCopy(FULL_BAR, UNICODE_BAR_CHAR_SIZE, dst, dst_end);

        size_t remainder = int_width % GRADES_IN_FULL_BAR;

        if (remainder)
            checkedCopy(&FRACTIONAL_BARS[(remainder - 1) * UNICODE_BAR_CHAR_SIZE], UNICODE_BAR_CHAR_SIZE, dst, dst_end);
    }

    std::string render(double width)
    {
        std::string res;
        res.resize(getWidthInBytes(width));
        render(width, res.data(), res.data() + res.size());
        return res;
    }
}
