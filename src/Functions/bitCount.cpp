#include <base/bit_cast.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <bit>


namespace DB
{

template <typename A>
struct BitCountImpl
{
    using ResultType = std::conditional_t<(sizeof(A) * 8 >= 256), UInt16, UInt8>;
    static constexpr bool allow_string_or_fixed_string = true;

    static ResultType apply(A a)
    {
        /// We count bits in the value representation in memory. For example, we support floats.
        /// We need to avoid sign-extension when converting signed numbers to larger type. So, uint8_t(-1) has 8 bits.

        if constexpr (is_big_int_v<A>)
        {
            ResultType res = 0;
            for (auto item : a.items)
                res += std::popcount(item);
            return res;
        }
        if constexpr (std::is_same_v<A, UInt64> || std::is_same_v<A, Int64>)
            return std::popcount(static_cast<UInt64>(a));
        if constexpr (std::is_same_v<A, UInt32> || std::is_same_v<A, Int32> || std::is_unsigned_v<A>)
            return std::popcount(static_cast<UInt32>(a));
        if constexpr (std::is_same_v<A, Int16>)
            return std::popcount(static_cast<UInt16>(a));
        if constexpr (std::is_same_v<A, Int8>)
            return std::popcount(static_cast<uint8_t>(a));
        else
            return std::popcount(bit_cast<uint64_t>(a));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameBitCount { static constexpr auto name = "bitCount"; };
using FunctionBitCount = FunctionUnaryArithmetic<BitCountImpl, NameBitCount, false /* is injective */>;

/// The function has no ranges of monotonicity.
template <> struct FunctionUnaryArithmeticMonotonicity<NameBitCount>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

REGISTER_FUNCTION(BitCount)
{
    factory.registerFunction<FunctionBitCount>();
}

}
