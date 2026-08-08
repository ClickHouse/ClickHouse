#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Common/StringUtils.h>

namespace DB
{
namespace
{

struct InitcapImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset next_offset = offsets[i];
            array(&data[prev_offset], &data[next_offset], &res_data[prev_offset]);
            prev_offset = next_offset;
        }
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t n, ColumnString::Chars & res_data, size_t input_rows_count)
    {
        res_data.resize(data.size());

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset next_offset = prev_offset + n;
            array(&data[prev_offset], &data[next_offset], &res_data[prev_offset]);
            prev_offset = next_offset;
        }
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        bool prev_alphanum = false;

        for (; src < src_end; ++src, ++dst)
        {
            char c = *src;
            bool alphanum = isAlphaNumericASCII(c);
            if (alphanum && !prev_alphanum)
            {
                if (isAlphaASCII(c))
                    *dst = toUpperIfAlphaASCII(c);
                else
                    *dst = c;
            }
            else if (isAlphaASCII(c))
                *dst = toLowerIfAlphaASCII(c);
            else
                *dst = c;
            prev_alphanum = alphanum;
        }
    }
};

struct NameInitcap
{
    static constexpr auto name = "initcap";
};
using FunctionInitcap = FunctionStringToString<InitcapImpl, NameInitcap>;

}

REGISTER_FUNCTION(Initcap)
{
    FunctionDocumentation::Description description = R"(
Converts the first letter of each word to upper case and the rest to lower case.
Words are sequences of alphanumeric characters separated by non-alphanumeric characters.

:::note
Because `initcap` converts only the first letter of each word to upper case you may observe unexpected behaviour for words containing apostrophes or capital letters.
This is a known behaviour and there are no plans to fix it currently.
:::
)";
    FunctionDocumentation::Syntax syntax = "initcap(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `s` with the first letter of each word converted to upper case.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT initcap('building for fast')",
        R"(
┌─initcap('building for fast')─┐
│ Building For Fast            │
└──────────────────────────────┘
        )"
    },
    {
        "Example of known behavior for words containing apostrophes or capital letters",
        "SELECT initcap('John''s cat won''t eat.');",
        R"(
┌─initcap('Joh⋯n\'t eat.')─┐
│ John'S Cat Won'T Eat.    │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionInitcap>(documentation, FunctionFactory::Case::Insensitive);
}

}
