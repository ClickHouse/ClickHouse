#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

template <typename A>
struct NegateImpl
{
    using ResultType = std::conditional_t<is_decimal<A>, A, typename NumberTraits::ResultOfNegate<A>::Type>;
    static constexpr const bool allow_string_or_fixed_string = false;

    static NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        return -static_cast<ResultType>(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        return arg->getType()->isIntegerTy() ? b.CreateNeg(arg) : b.CreateFNeg(arg);
    }
#endif
};

struct NameNegate { static constexpr auto name = "negate"; };
using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;

template <> struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        /// negate(UInt64) -> Int64:
        ///  * monotonically decreases on [0, 2^63] (no overflow),
        ///  * then jumps up from -2^63 to 2^63-1, then
        ///  * monotonically decreases on [2^63+1, 2^64-1] (with overflow).
        /// Similarly for UInt128 and UInt256.
        /// Note: we currently don't handle the corner case -UINT64_MIN == UINT64_MIN, and similar for floats and wide signed ints.
        /// (This implementation seems overcomplicated and not very correct, maybe there's a better way to do it,
        ///  maybe by using the actual IDataType instead of two field types.)

        /// We don't know the data type, assume nonmonotonic.
        if (left.isNull() && right.isNull())
            return { .is_monotonic = false, .is_positive = false, .is_strict = true };

        auto which_half_if_unsigned_or_infinity = [](const Field & f, int half_if_null, bool & is_unsigned) -> int
        {
            is_unsigned = true;
            switch (f.getType())
            {
                case Field::Types::UInt64: return (f.safeGet<UInt64>() > 1ul << 63) ? +1 : -1;
                case Field::Types::UInt128: return (f.safeGet<UInt128>() > UInt128(1) << 127) ? +1 : -1;
                case Field::Types::UInt256: return (f.safeGet<UInt256>() > UInt256(1) << 255) ? +1 : -1;
                default: break;
            }
            is_unsigned = false;
            if (f.isPositiveInfinity())
                return +1;
            if (f.isNegativeInfinity())
                return -1;
            if (f.isNull())
                return half_if_null;
            return 0;
        };
        bool left_is_unsigned, right_is_unsigned;
        int left_half = which_half_if_unsigned_or_infinity(left, -1, left_is_unsigned);
        int right_half = which_half_if_unsigned_or_infinity(right, +1, right_is_unsigned);

        bool is_monotonic = true;
        if (left_is_unsigned || right_is_unsigned)
            is_monotonic = left_half * right_half >= 0; /// either both values in the same half or unexpected signed-unsigned mix
        return { .is_monotonic = is_monotonic, .is_positive = false, .is_strict = true };
    }
};

REGISTER_FUNCTION(Negate)
{
    factory.registerFunction<FunctionNegate>();
}

}
