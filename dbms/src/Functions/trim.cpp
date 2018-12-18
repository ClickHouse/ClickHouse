#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

#if __SSE4_2__
#include <nmmintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct TrimModeLeft
{
    static constexpr auto name = "trimLeft";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = false;
};

struct TrimModeRight
{
    static constexpr auto name = "trimRight";
    static constexpr bool trim_left = false;
    static constexpr bool trim_right = true;
};

struct TrimModeBoth
{
    static constexpr auto name = "trimBoth";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = true;
};

template <typename mode>
class FunctionTrimImpl
{
public:
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        const UInt8 * start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
        {
            execute(reinterpret_cast<const UInt8 *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);

            res_data.resize(res_data.size() + length + 1);
            std::memcpy(&res_data[res_offset], start, length);
            res_offset += length + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Functions trimLeft, trimRight and trimBoth cannot work with FixedString argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    static void execute(const UInt8 * data, size_t size, const UInt8 *& res_data, size_t & res_size)
    {
        size_t chars_to_trim_left = 0;
        size_t chars_to_trim_right = 0;
        char whitespace = ' ';
#if __SSE4_2__
        const auto bytes_sse = sizeof(__m128i);
        const auto size_sse = size - (size % bytes_sse);
        const auto whitespace_mask = _mm_set1_epi8(whitespace);
        constexpr auto base_sse_mode = _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_NEGATIVE_POLARITY;
        auto mask = bytes_sse;
#endif

        if constexpr (mode::trim_left)
        {
#if __SSE4_2__
            /// skip whitespace from left in blocks of up to 16 characters
            constexpr auto left_sse_mode = base_sse_mode | _SIDD_LEAST_SIGNIFICANT;
            while (mask == bytes_sse && chars_to_trim_left < size_sse)
            {
                const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + chars_to_trim_left));
                mask = _mm_cmpistri(whitespace_mask, chars, left_sse_mode);
                chars_to_trim_left += mask;
            }
#endif
            /// skip remaining whitespace from left, character by character
            while (chars_to_trim_left < size && data[chars_to_trim_left] == whitespace)
                ++chars_to_trim_left;
        }

        if constexpr (mode::trim_right)
        {
            constexpr auto right_sse_mode = base_sse_mode | _SIDD_MOST_SIGNIFICANT;
            const auto trim_right_size = size - chars_to_trim_left;
#if __SSE4_2__
            /// try to skip whitespace from right in blocks of up to 16 characters
            const auto trim_right_size_sse = trim_right_size - (trim_right_size % bytes_sse);
            while (mask == bytes_sse && chars_to_trim_right < trim_right_size_sse)
            {
                const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + size - chars_to_trim_right - bytes_sse));
                mask = _mm_cmpistri(whitespace_mask, chars, right_sse_mode);
                chars_to_trim_right += mask;
            }
#endif
            /// skip remaining whitespace from right, character by character
            while (chars_to_trim_right < trim_right_size && data[size - chars_to_trim_right - 1] == whitespace)
                ++chars_to_trim_right;
        }

        res_data = data + chars_to_trim_left;
        res_size = size - chars_to_trim_left - chars_to_trim_right;
    }
};

using FunctionTrimLeft = FunctionStringToString<FunctionTrimImpl<TrimModeLeft>, TrimModeLeft>;
using FunctionTrimRight = FunctionStringToString<FunctionTrimImpl<TrimModeRight>, TrimModeRight>;
using FunctionTrimBoth = FunctionStringToString<FunctionTrimImpl<TrimModeBoth>, TrimModeBoth>;

void registerFunctionTrim(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTrimLeft>();
    factory.registerFunction<FunctionTrimRight>();
    factory.registerFunction<FunctionTrimBoth>();
}
}
