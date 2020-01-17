#include <ext/bit_cast.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>


namespace DB
{

template <typename A>
struct BitCountImpl
{
    using ResultType = UInt8;

    static inline ResultType apply(A a)
    {
        /// We count bits in the value representation in memory. For example, we support floats.
        /// We need to avoid sign-extension when converting signed numbers to larger type. So, uint8_t(-1) has 8 bits.

        return __builtin_popcountll(ext::bit_cast<unsigned long long>(a));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameBitCount { static constexpr auto name = "bitCount"; };
using FunctionBitCount = FunctionUnaryArithmetic<BitCountImpl, NameBitCount, true>;

/// The function has no ranges of monotonicity.
template <> struct FunctionUnaryArithmeticMonotonicity<NameBitCount>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

void registerFunctionBitCount(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitCount>();
}

}
