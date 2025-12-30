#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

template <typename A, typename B>
struct BitHammingDistanceImpl
{
    using ResultType = std::conditional_t<(sizeof(A) * 8 >= 256), UInt16, UInt8>;
    static constexpr bool allow_fixed_string = true;
    static constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        /// Note: it's unspecified if signed integers should be promoted with sign-extension or with zero-fill.
        /// This behavior can change in the future.

        if constexpr (sizeof(A) <= sizeof(UInt64) && sizeof(B) <= sizeof(UInt64))
        {
            UInt64 res = static_cast<UInt64>(a) ^ static_cast<UInt64>(b);
            return std::popcount(res);
        }
        else if constexpr (is_big_int_v<A> && is_big_int_v<B>)
        {
            auto xored = a ^ b;

            ResultType res = 0;
            for (auto item : xored.items)
                res += std::popcount(item);
            return res;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unsupported data type combination in function 'bitHammingDistance'");
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// special type handling, some other time
#endif
};

struct NameBitHammingDistance
{
    static constexpr auto name = "bitHammingDistance";
};
using FunctionBitHammingDistance = BinaryArithmeticOverloadResolver<BitHammingDistanceImpl, NameBitHammingDistance>;

REGISTER_FUNCTION(BitHammingDistance)
{
    FunctionDocumentation::Description description = R"(
Returns the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) between the bit representations of two numbers.
Can be used with [`SimHash`](../../sql-reference/functions/hash-functions.md#ngramSimHash) functions for detection of semi-duplicate strings.
The smaller the distance, the more similar the strings are.
)";
    FunctionDocumentation::Syntax syntax = "bitHammingDistance(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "First number for Hamming distance calculation.", {"(U)Int*", "Float*"}},
        {"y", "Second number for Hamming distance calculation.", {"(U)Int*", "Float*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the hamming distance between `x` and `y`", {"UInt8"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT bitHammingDistance(111, 121);",
        R"(
┌─bitHammingDistance(111, 121)─┐
│                            3 │
└──────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitHammingDistance>(documentation);
}
}
