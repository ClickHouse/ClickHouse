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
    FunctionDocumentation::Description description = R"(
Counts the number of distinct bytes in a string.
)";
    FunctionDocumentation::Syntax syntax = "stringBytesUniq(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to analyze.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of distinct bytes in the string.", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT stringBytesUniq('Hello')",
        R"(
┌─stringBytesUniq('Hello')─┐
│                        4 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStringBytesUniq>(documentation);
}

}
