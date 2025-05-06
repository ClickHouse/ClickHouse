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
    FunctionDocumentation::Description description = "Calculates Shannon's entropy of byte distribution in a string.";
    FunctionDocumentation::Syntax syntax = "stringBytesEntropy(s);";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to analyze. [String](../../sql-reference/data-types/string.md))"}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The Shannon entropy of the byte distribution. [Float64](../../sql-reference/data-types/float.md).";
    FunctionDocumentation::Examples examples = {
        {"Example",
         "SELECT stringBytesEntropy('Hello, world!');",
         "3.180832987205441"}
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;

    FunctionDocumentation function_documentation = {
        .description = description,
        .syntax = syntax,
        .arguments = arguments,
        .returned_value = returned_value,
        .examples = examples,
        .category = category
    };

    factory.registerFunction<FunctionStringBytesEntropy>(function_documentation);
}

}
