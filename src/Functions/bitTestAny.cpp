#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBitTestMany.h>

namespace DB
{
namespace
{

struct BitTestAnyImpl
{
    template <typename A, typename B>
    static UInt8 apply(A a, B b) { return (a & b) != 0; }
};

struct NameBitTestAny { static constexpr auto name = "bitTestAny"; };
using FunctionBitTestAny = FunctionBitTestMany<BitTestAnyImpl, NameBitTestAny>;

}

REGISTER_FUNCTION(BitTestAny)
{
    FunctionDocumentation::Description description = R"(
Returns result of the [logical disjunction](https://en.wikipedia.org/wiki/Logical_disjunction) (OR operator) of all bits at the given positions in a number.
Counts right-to-left, starting at 0.

The logical OR between two bits is true if at least one of the input bits is true.
    )";
    FunctionDocumentation::Syntax syntax = "bitTestAny(a, index1[, index2, ... , indexN])";
    FunctionDocumentation::Arguments arguments = {
        {"a", "An integer value.", {"(U)Int8/16/32/64"}},
        {"index1, ...", "One or multiple positions of bits.", {"(U)Int8/16/32/64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the result of the logical disjunction", {"UInt8"}};
    FunctionDocumentation::Examples examples = {{"Usage example 1", "SELECT bitTestAny(43, 0, 2);",
        R"(
┌─bin(43)──┬─bitTestAny(43, 0, 2)─┐
│ 00101011 │                    1 │
└──────────┴──────────────────────┘
        )"}, {"Usage example 2", "SELECT bitTestAny(43, 4, 2);",
    R"(
┌─bin(43)──┬─bitTestAny(43, 4, 2)─┐
│ 00101011 │                    0 │
└──────────┴──────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitTestAny>(documentation);
}

}
