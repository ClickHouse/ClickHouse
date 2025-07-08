#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <base/int8_to_string.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
}

namespace
{

template <typename A, typename B>
struct BitTestImpl
{
    using ResultType = UInt8;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "bitTest is not implemented for big integers as second argument");
        else
        {
            typename NumberTraits::ToInteger<A>::Type a_int = a;
            typename NumberTraits::ToInteger<B>::Type b_int = b;
            const auto max_position = static_cast<decltype(b)>((8 * sizeof(a)) - 1);
            if (b_int > max_position || b_int < 0)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                                "The bit position argument needs to a positive value and less or equal to {} for integer {}",
                                std::to_string(max_position), std::to_string(a_int));
            return (a_int >> b_int) & 1;
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO
#endif
};

struct NameBitTest { static constexpr auto name = "bitTest"; };
using FunctionBitTest = BinaryArithmeticOverloadResolver<BitTestImpl, NameBitTest, true, false>;

}

REGISTER_FUNCTION(BitTest)
{
    FunctionDocumentation::Description description = "Takes any number and converts it into [binary form](https://en.wikipedia.org/wiki/Binary_number), then returns the value of the bit at a specified position. Counting is done right-to-left, starting at 0.";
    FunctionDocumentation::Syntax syntax = "bitTest(a, i)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "Number to convert.", {"(U)Int8/16/32/64", "Float*"}},
        {"i", "Position of the bit to return.", {"(U)Int8/16/32/64", "Float*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the value of the bit at position `i` in the binary representation of `a`", {"UInt8"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT bin(2), bitTest(2, 1);",
        R"(
┌─bin(2)───┬─bitTest(2, 1)─┐
│ 00000010 │             1 │
└──────────┴───────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitTest>(documentation);
}

}
