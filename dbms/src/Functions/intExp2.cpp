#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Common/FieldVisitors.h>
#include <Common/intExp.h>

namespace DB
{

template <typename A>
struct IntExp2Impl
{
    using ResultType = UInt64;
    static constexpr const bool allow_fixed_string = false;

    static inline ResultType apply(A a)
    {
        return intExp2(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception("IntExp2Impl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateShl(llvm::ConstantInt::get(arg->getType(), 1), arg);
    }
#endif
};

/// Assumed to be injective for the purpose of query optimization, but in fact it is not injective because of possible overflow.
struct NameIntExp2 { static constexpr auto name = "intExp2"; };
using FunctionIntExp2 = FunctionUnaryArithmetic<IntExp2Impl, NameIntExp2, true>;

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp2>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 63)
            return {};

        return { true };
    }
};

void registerFunctionIntExp2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp2>();
}

}
