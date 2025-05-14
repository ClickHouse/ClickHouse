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
    factory.registerFunction<FunctionBitHammingDistance>();
}
}
