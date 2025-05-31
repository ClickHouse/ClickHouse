#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/stringBytes.h>
#include <Functions/IFunction.h>
#include <Common/BitHelpers.h>
#include <Common/PODArray.h>

#include <bit>
#include <cmath>

namespace DB
{

struct StringBytesUniqImpl
{
    using ResultType = UInt16;

    static ResultType process(const UInt8 * data, size_t size)
    {
        UInt64 mask[4] = {0};
        const UInt8 * end = data + size;

        for (; data < end; ++data)
        {
            UInt8 byte = *data;
            mask[byte >> 6] |= (1ULL << (byte & 0x3F));
        }

        return std::popcount(mask[0]) + std::popcount(mask[1]) + std::popcount(mask[2]) + std::popcount(mask[3]);
    }
};


struct NameStringBytesUniq
{
    static constexpr auto name = "stringBytesUniq";
};


using FunctionStringBytesUniq = FunctionStringBytes<StringBytesUniqImpl, NameStringBytesUniq>;

REGISTER_FUNCTION(StringBytesUniq)
{
    FunctionDocumentation::Description description = "Counts the number of distinct bytes in a string.";
    FunctionDocumentation::Syntax syntax = "stringBytesUniq(s);";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to analyze. [String](../../sql-reference/data-types/string.md)"}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The number of distinct bytes in the string. [UInt16](../../sql-reference/data-types/int-uint.md).";
    FunctionDocumentation::Examples examples = {
        {"Example", "SELECT stringBytesUniq('Hello, world!');", "10"}
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};

    FunctionDocumentation function_documentation = {
        .description = description,
        .syntax = syntax,
        .arguments = arguments,
        .returned_value = returned_value,
        .examples = examples,
        .introduced_in = introduced_in,
        .category = category
    };

    factory.registerFunction<FunctionStringBytesUniq>(function_documentation);
}

}
