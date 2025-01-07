#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <base/find_symbols.h>
#include <base/hex.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

static size_t encodeURL(const char * __restrict src, size_t src_size, char * __restrict dst, bool space_as_plus)
{
    char * dst_pos = dst;
    for (size_t i = 0; i < src_size; ++i)
    {
        if ((src[i] >= '0' && src[i] <= '9') || (src[i] >= 'a' && src[i] <= 'z') || (src[i] >= 'A' && src[i] <= 'Z')
            || src[i] == '-' || src[i] == '_' || src[i] == '.' || src[i] == '~')
        {
            *dst_pos = src[i];
            ++dst_pos;
        }
        else if (src[i] == ' ' && space_as_plus)
        {
            *dst_pos = '+';
            ++dst_pos;
        }
        else
        {
            dst_pos[0] = '%';
            ++dst_pos;
            writeHexByteUppercase(src[i], dst_pos);
            dst_pos += 2;
        }
    }
    *dst_pos = 0;
    ++dst_pos;
    return dst_pos - dst;
}


/// We assume that size of the dst buf isn't less than src_size.
static size_t decodeURL(const char * __restrict src, size_t src_size, char * __restrict dst, bool plus_as_space)
{
    const char * src_prev_pos = src;
    const char * src_curr_pos = src;
    const char * src_end = src + src_size;
    char * dst_pos = dst;

    while (true)
    {
        src_curr_pos = find_first_symbols<'%', '+'>(src_curr_pos, src_end);

        if (src_curr_pos == src_end)
        {
            break;
        }
        if (*src_curr_pos == '+')
        {
            if (!plus_as_space)
            {
                ++src_curr_pos;
                continue;
            }
            size_t bytes_to_copy = src_curr_pos - src_prev_pos;
            memcpySmallAllowReadWriteOverflow15(dst_pos, src_prev_pos, bytes_to_copy);
            dst_pos += bytes_to_copy;

            ++src_curr_pos;
            src_prev_pos = src_curr_pos;
            *dst_pos = ' ';
            ++dst_pos;
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

enum URLCodeStrategy
{
    encode,
    decode
};

/// Percent decode of URL data.
template <URLCodeStrategy code_strategy, bool space_as_plus>
struct CodeURLComponentImpl
{
    static void vector(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (code_strategy == encode)
        {
            /// the destination(res_data) string is at most three times the length of the source string
            res_data.resize(data.size() * 3);
        }
        else
        {
            res_data.resize(data.size());
        }

        res_offsets.resize(input_rows_count);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
            size_t src_size = offsets[i] - prev_offset;
            size_t dst_size;

            if constexpr (code_strategy == encode)
            {
                /// Skip encoding of zero terminated character
                size_t src_encode_size = src_size - 1;
                dst_size = encodeURL(src_data, src_encode_size, reinterpret_cast<char *>(res_data.data() + res_offset), space_as_plus);
            }
            else
            {
                dst_size = decodeURL(src_data, src_size, reinterpret_cast<char *>(res_data.data() + res_offset), space_as_plus);
            }

            res_offset += dst_size;
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }

        res_data.resize(res_offset);
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by URL functions");
    }
};


struct NameDecodeURLComponent { static constexpr auto name = "decodeURLComponent"; };
struct NameEncodeURLComponent { static constexpr auto name = "encodeURLComponent"; };
struct NameDecodeURLFormComponent { static constexpr auto name = "decodeURLFormComponent"; };
struct NameEncodeURLFormComponent { static constexpr auto name = "encodeURLFormComponent"; };
using FunctionDecodeURLComponent = FunctionStringToString<CodeURLComponentImpl<decode, false>, NameDecodeURLComponent>;
using FunctionEncodeURLComponent = FunctionStringToString<CodeURLComponentImpl<encode, false>, NameEncodeURLComponent>;
using FunctionDecodeURLFormComponent = FunctionStringToString<CodeURLComponentImpl<decode, true>, NameDecodeURLFormComponent>;
using FunctionEncodeURLFormComponent = FunctionStringToString<CodeURLComponentImpl<encode, true>, NameEncodeURLFormComponent>;

REGISTER_FUNCTION(EncodeAndDecodeURLComponent)
{
    factory.registerFunction<FunctionDecodeURLComponent>();
    factory.registerFunction<FunctionEncodeURLComponent>();
    factory.registerFunction<FunctionDecodeURLFormComponent>();
    factory.registerFunction<FunctionEncodeURLFormComponent>();
}

}
