#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/FunctionFactory.h>
#include <Poco/Unicode.h>
#include <Common/UTF8Helpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

struct InitcapUTF8Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t /*input_rows_count*/)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);

        array(data.data(), data.data() + data.size(), offsets, res_data.data());
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function initcapUTF8 cannot work with FixedString argument");
    }

    static void processCodePoint(const UInt8 *& src, const UInt8 * src_end, UInt8 *& dst, bool& prev_alphanum)
    {
        size_t src_sequence_length = UTF8::seqLength(*src);
        auto src_code_point = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(src), src_end - src);

        if (src_code_point)
        {
            bool alpha = Poco::Unicode::isAlpha(*src_code_point);
            bool alphanum = alpha || Poco::Unicode::isDigit(*src_code_point);

            int dst_code_point = *src_code_point;
            if (alphanum && !prev_alphanum)
            {
                if (alpha)
                    dst_code_point = Poco::Unicode::toUpper(*src_code_point);
            }
            else if (alpha)
            {
                dst_code_point = Poco::Unicode::toLower(*src_code_point);
            }
            prev_alphanum = alphanum;
            if (dst_code_point > 0)
            {
                size_t dst_sequence_length = UTF8::convertCodePointToUTF8(dst_code_point, reinterpret_cast<char *>(dst), src_end - src);
                assert(dst_sequence_length <= 4);

                if (dst_sequence_length == src_sequence_length)
                {
                    src += dst_sequence_length;
                    dst += dst_sequence_length;
                    return;
                }
            }
        }

        *dst = *src;
        ++dst;
        ++src;
        prev_alphanum = false;
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, const ColumnString::Offsets & offsets, UInt8 * dst)
    {
        const auto * offset_it = offsets.begin();
        const UInt8 * begin = src;

        /// Handle remaining symbols, row by row (to avoid influence of bad UTF8 symbols from one row to another)
        while (src < src_end)
        {
            const UInt8 * row_end = begin + *offset_it;
            chassert(row_end >= src);
            bool prev_alphanum = false;
            while (src < row_end)
                processCodePoint(src, row_end, dst, prev_alphanum);
            ++offset_it;
        }
    }
};

struct NameInitcapUTF8
{
    static constexpr auto name = "initcapUTF8";
};

using FunctionInitcapUTF8 = FunctionStringToString<InitcapUTF8Impl, NameInitcapUTF8>;

}

REGISTER_FUNCTION(InitcapUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [`initcap`](#initcap), `initcapUTF8` converts the first letter of each word to upper case and the rest to lower case.
Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.

:::note
This function does not detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I).
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
:::
)";
    FunctionDocumentation::Syntax syntax = "initcapUTF8(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `s` with the first letter of each word converted to upper case.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT initcapUTF8('не тормозит')",
        R"(
┌─initcapUTF8('не тормозит')─┐
│ Не Тормозит                │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionInitcapUTF8>(documentation);
}

}
