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
        bool is_monotonic = true;
        switch (left.getType())
        {
            case Field::Types::UInt64:
                is_monotonic = (left.safeGet<UInt64>() > 1ul << 63) == (right.safeGet<UInt64>() > 1ul << 63);
                break;
            case Field::Types::UInt128:
                is_monotonic = (left.safeGet<UInt128>() > UInt128(1) << 127) == (right.safeGet<UInt128>() > UInt128(1) << 127);
                break;
            case Field::Types::UInt256:
                is_monotonic = (left.safeGet<UInt256>() > UInt256(1) << 255) == (right.safeGet<UInt256>() > UInt256(1) << 255);
                break;
            default:
                break;
        }

        return { .is_monotonic = is_monotonic, .is_positive = false, .is_strict = true };
    }
};

REGISTER_FUNCTION(Negate)
{
    factory.registerFunction<FunctionNegate>();
}

}
