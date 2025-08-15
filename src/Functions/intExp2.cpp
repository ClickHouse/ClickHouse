#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/intExp.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename A>
struct IntExp2Impl
{
    using ResultType = UInt64;
    static constexpr bool allow_string_or_fixed_string = false;

    static ResultType apply([[maybe_unused]] A a)
    {
        if constexpr (is_big_int_v<A>)
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "intExp2 not implemented for big integers");
        else
            return intExp2(static_cast<int>(a));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IntExp2Impl expected an integral type");
        return b.CreateShl(llvm::ConstantInt::get(arg->getType(), 1), arg);
    }
#endif
};

/// Assumed to be injective for the purpose of query optimization, but in fact it is not injective because of possible overflow.
struct NameIntExp2 { static constexpr auto name = "intExp2"; };
using FunctionIntExp2 = FunctionUnaryArithmetic<IntExp2Impl, NameIntExp2, true>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp2>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 63)
            return {};

        return { .is_monotonic = true, .is_strict = true, };
    }
};

REGISTER_FUNCTION(IntExp2)
{
    factory.registerFunction<FunctionIntExp2>();
}

}
