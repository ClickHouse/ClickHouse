#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Common/encodingURL.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
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
