#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Functions/reverse.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/** Reverse the sequence of code points in a UTF-8 encoded string.
  * The result may not match the expected result, because modifying code points (for example, diacritics) may be applied to another symbols.
  * If the string is not encoded in UTF-8, then the behavior is undefined.
  */
struct ReverseUTF8Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        bool all_ascii = isAllASCII(data.data(), data.size());
        if (all_ascii)
        {
            ReverseImpl::vector(data, offsets, res_data, res_offsets, input_rows_count);
            return;
        }

        res_data.resize(data.size());
        res_offsets.assign(offsets);

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset j = prev_offset;
            while (j < offsets[i])
            {
                if (data[j] < 0xC0)
                {
                    res_data[offsets[i] + prev_offset - 1 - j] = data[j];
                    j += 1;
                }
                else if (data[j] < 0xE0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 1 - j - 1], &data[j], 2);
                    j += 2;
                }
                else if (data[j] < 0xF0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 1 - j - 2], &data[j], 3);
                    j += 3;
                }
                else
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 1 - j - 3], &data[j], 4);
                    j += 4;
                }
            }

            prev_offset = offsets[i];
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot apply function reverseUTF8 to fixed string.");
    }
};

struct NameReverseUTF8
{
    static constexpr auto name = "reverseUTF8";
};
using FunctionReverseUTF8 = FunctionStringToString<ReverseUTF8Impl, NameReverseUTF8, true>;

}

REGISTER_FUNCTION(ReverseUTF8)
{
    FunctionDocumentation::Description description = R"(
Reverses a sequence of Unicode code points in a string.
Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.
)";
    FunctionDocumentation::Syntax syntax = "reverseUTF8(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String containing valid UTF-8 encoded text.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string with the sequence of Unicode code points reversed.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT reverseUTF8('ClickHouse')",
        "esuoHkcilC"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReverseUTF8>(documentation);
}

}
