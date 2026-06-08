#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBitTestMany.h>

namespace DB
{
namespace
{

struct BitTestAllImpl
{
    template <typename A, typename B>
    static UInt8 apply(A a, B b) { return (a & b) == b; }
};

struct NameBitTestAll { static constexpr auto name = "bitTestAll"; };
using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

}

REGISTER_FUNCTION(BitTestAll)
{
    FunctionDocumentation::Description description = R"(
Returns result of the [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) (AND operator) of all bits at the given positions.
Counts right-to-left, starting at 0.

The logical AND between two bits is true if and only if both input bits are true.
    )";
    FunctionDocumentation::Syntax syntax = "bitTestAll(a, index1[, index2, ... , indexN])";
    FunctionDocumentation::Arguments arguments = {
        {"a", "An integer value.", {"(U)Int8/16/32/64"}},
        {"index1, ...", "One or multiple positions of bits.", {"(U)Int8/16/32/64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the result of the logical conjunction", {"UInt8"}};
    FunctionDocumentation::Examples examples = {{"Usage example 1", "SELECT bitTestAll(43, 0, 1, 3, 5);",
        R"(
┌─bin(43)──┬─bitTestAll(43, 0, 1, 3, 5)─┐
│ 00101011 │                          1 │
└──────────┴────────────────────────────┘
        )"}, {"Usage example 2", "SELECT bitTestAll(43, 0, 1, 3, 5, 2);",
    R"(
┌─bin(43)──┬─bitTestAll(4⋯1, 3, 5, 2)─┐
│ 00101011 │                        0 │
└──────────┴──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitTestAll>(documentation);
}

}
