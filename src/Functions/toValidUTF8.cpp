#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <IO/writeValidUTF8.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

struct ToValidUTF8Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        /// It can be larger than that, but we believe it is unlikely to happen.
        res_data.resize(data.size());
        res_offsets.resize(input_rows_count);

        size_t prev_offset = 0;
        WriteBufferFromVector<ColumnString::Chars> write_buffer(res_data);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * haystack_data = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t haystack_size = offsets[i] - prev_offset;
            writeValidUTF8(haystack_data, haystack_data + haystack_size, write_buffer);
            res_offsets[i] = write_buffer.count();
            prev_offset = offsets[i];
        }
        write_buffer.finalize();
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by toValidUTF8 function");
    }
};

struct NameToValidUTF8
{
    static constexpr auto name = "toValidUTF8";
};
using FunctionToValidUTF8 = FunctionStringToString<ToValidUTF8Impl, NameToValidUTF8>;

}

REGISTER_FUNCTION(ToValidUTF8)
{
    FunctionDocumentation::Description description = R"(
Converts a string to valid UTF-8 encoding by replacing any invalid UTF-8 characters with the replacement character `�` (U+FFFD).
When multiple consecutive invalid characters are found, they are collapsed into a single replacement character.
)";
    FunctionDocumentation::Syntax syntax = "toValidUTF8(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Any set of bytes represented as the String data type object.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a valid UTF-8 string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(SELECT toValidUTF8('\\x61\\xF0\\x80\\x80\\x80b'))",
        R"(\\x61\\xF0\\x80\\x80\\x80b
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToValidUTF8>(documentation);
}

}
