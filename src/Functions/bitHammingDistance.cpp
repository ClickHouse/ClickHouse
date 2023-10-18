#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <bit>

namespace DB
{
template <typename A, typename B>
struct BitHammingDistanceImpl
{
    using ResultType = UInt8;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        UInt64 res = static_cast<UInt64>(a) ^ static_cast<UInt64>(b);
        return std::popcount(res);
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
    factory.registerFunction<FunctionBitHammingDistance>();
}
}
