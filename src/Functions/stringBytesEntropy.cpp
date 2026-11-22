#include <Functions/FunctionFactory.h>
#include <Functions/stringBytes.h>

#include <algorithm>
#include <cmath>

namespace DB
{

struct StringBytesEntropyImpl
{
    using ResultType = Float64;

    /// Zero between rows: `process` resets every counter it increments (they all lie in [min_byte, max_byte]).
    std::array<UInt32, 256> counters{};

    ResultType process(const UInt8 * data, size_t size)
    {
        if (size == 0)
            return 0;

        const UInt8 * end = data + size;

        UInt8 min_byte = 255;
        UInt8 max_byte = 0;
        for (const UInt8 * pos = data; pos < end; ++pos)
        {
            min_byte = std::min(min_byte, *pos);
            max_byte = std::max(max_byte, *pos);
        }

        for (; data < end; ++data)
            counters[*data]++;

        Float64 entropy = 0.0;

        for (size_t byte = min_byte; byte <= max_byte; ++byte)
        {
            UInt32 count = counters[byte];
            if (count > 0)
            {
                counters[byte] = 0;
                Float64 p = static_cast<Float64>(count) / static_cast<Float64>(size);
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
│                   3.180832987205441 │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStringBytesEntropy>(documentation);
}

}
