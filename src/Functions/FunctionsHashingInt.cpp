#include <Functions/FunctionsHashing.h>

#include <Functions/FunctionFactory.h>

/// FunctionsHashing instantiations are separated into files FunctionsHashing*.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{

REGISTER_FUNCTION(HashingInt)
{
    FunctionDocumentation::Description intHash32_description = R"(
Calculates a 32-bit hash of an integer.

The hash function is relatively fast but not cryptographic hash function.
)";
    FunctionDocumentation::Syntax intHash32_syntax = "intHash32(arg)";
    FunctionDocumentation::Arguments intHash32_arguments = {
        {"arg", "Integer to hash.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue intHash32_returned_value = {"Returns the computed 32-bit hash code of the input integer", {"UInt32"}};
    FunctionDocumentation::Examples intHash32_examples = {
    {
        "Usage example",
        "SELECT intHash32(42);",
        R"(
┌─intHash32(42)─┐
│    1228623923 │
└───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn intHash32_introduced_in = {1, 1};
    FunctionDocumentation::Category intHash32_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation intHash32_documentation = {intHash32_description, intHash32_syntax, intHash32_arguments, intHash32_returned_value, intHash32_examples, intHash32_introduced_in, intHash32_category};
    factory.registerFunction<FunctionIntHash32>(intHash32_documentation);

    FunctionDocumentation::Description intHash64_description = R"(
Calculates a 64-bit hash of an integer.

The hash function is relatively fast (even faster than [`intHash32`](#intHash32)) but not a cryptographic hash function.
)";
    FunctionDocumentation::Syntax intHash64_syntax = "intHash64(int)";
    FunctionDocumentation::Arguments intHash64_arguments = {
        {"int", "Integer to hash.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue intHash64_returned_value = {"64-bit hash code.", {"UInt64"}};
    FunctionDocumentation::Examples intHash64_examples = {
    {
        "Usage example",
        "SELECT intHash64(42);",
        R"(
┌────────intHash64(42)─┐
│ 11490350930367293593 │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn intHash64_introduced_in = {1, 1};
    FunctionDocumentation::Category intHash64_category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation intHash64_documentation = {intHash64_description, intHash64_syntax, intHash64_arguments, intHash64_returned_value, intHash64_examples, intHash64_introduced_in, intHash64_category};
    factory.registerFunction<FunctionIntHash64>(intHash64_documentation);
}
}
