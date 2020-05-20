#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

template <typename A>
struct NegateImpl
{
    using ResultType = std::conditional_t<IsDecimalNumber<A>, A, typename NumberTraits::ResultOfNegate<A>::Type>;
    static constexpr const bool allow_fixed_string = false;

    static inline NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        return -static_cast<ResultType>(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
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
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return { true, false };
    }
};

void registerFunctionNegate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNegate>();
}

}
