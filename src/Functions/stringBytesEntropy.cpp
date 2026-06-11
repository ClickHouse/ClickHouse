#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/stringBytes.h>
#include <Common/BitHelpers.h>
#include <Common/PODArray.h>

#include <cmath>

namespace DB
{

struct StringBytesEntropyImpl
{
    using ResultType = Float64;

    static ResultType process(const UInt8 * data, size_t size)
    {
        if (size == 0)
            return 0;

        std::array<UInt32, 256> counters{};
        const UInt8 * end = data + size;

        for (; data < end; ++data)
            counters[*data]++;

        Float64 entropy = 0.0;

        for (size_t byte = 0; byte < 256; ++byte)
        {
            UInt32 count = counters[byte];
            if (count > 0)
            {
                Float64 p = static_cast<Float64>(count) / size;
                entropy -= p * std::log2(p);
            }
        }

        return entropy;
    }
};

struct NameStringBytesEntropy
{
    static constexpr auto name = "stringBytesEntropy";
};

using FunctionStringBytesEntropy = FunctionStringBytes<StringBytesEntropyImpl, NameStringBytesEntropy>;

REGISTER_FUNCTION(StringBytesEntropy)
{
    FunctionDocumentation::Description description = R"(
Calculates Shannon's entropy of byte distribution in a string.
)";
    FunctionDocumentation::Syntax syntax = "stringBytesEntropy(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to analyze.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns Shannon's entropy of byte distribution in the string.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT stringBytesEntropy('Hello, world!')",
        R"(
┌─stringBytesEntropy('Hello, world!')─┐
│                         3.07049960  │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStringBytesEntropy>(documentation);
}

}
