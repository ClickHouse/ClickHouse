#include <Common/hex.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <common/find_symbols.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/// We assume that size of the dst buf isn't less than src_size.
static size_t decodeURL(const char * src, size_t src_size, char * dst)
{
    const char * src_prev_pos = src;
    const char * src_curr_pos = src;
    const char * src_end = src + src_size;
    char * dst_pos = dst;

    while (true)
    {
        src_curr_pos = find_first_symbols<'%'>(src_curr_pos, src_end);

        if (src_curr_pos == src_end)
        {
            break;
        }
        else if (src_end - src_curr_pos < 3)
        {
            src_curr_pos = src_end;
            break;
        }
        else
        {
            unsigned char high = unhex(src_curr_pos[1]);
            unsigned char low = unhex(src_curr_pos[2]);

            if (high != 0xFF && low != 0xFF)
            {
                unsigned char octet = (high << 4) + low;

                size_t bytes_to_copy = src_curr_pos - src_prev_pos;
                memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
                dst_pos += bytes_to_copy;

                *dst_pos = octet;
                ++dst_pos;

                src_prev_pos = src_curr_pos + 3;
            }

            src_curr_pos += 3;
        }
    }

    if (src_prev_pos < src_curr_pos)
    {
        size_t bytes_to_copy = src_curr_pos - src_prev_pos;
        memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
        dst_pos += bytes_to_copy;
    }

    return dst_pos - dst;
}


/// Percent decode of URL data.
struct DecodeURLComponentImpl
{
    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
            size_t src_size = offsets[i] - prev_offset;
            size_t dst_size = decodeURL(src_data, src_size, reinterpret_cast<char *>(res_data.data() + res_offset));

            res_offset += dst_size;
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }

        res_data.resize(res_offset);
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
    }
};


struct NameDecodeURLComponent { static constexpr auto name = "decodeURLComponent"; };
using FunctionDecodeURLComponent = FunctionStringToString<DecodeURLComponentImpl, NameDecodeURLComponent>;

void registerFunctionDecodeURLComponent(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDecodeURLComponent>();
}

}
