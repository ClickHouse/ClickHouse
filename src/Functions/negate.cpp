#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

template <typename A>
struct NegateImpl
{
    using ResultType = std::conditional_t<is_decimal<A>, A, typename NumberTraits::ResultOfNegate<A>::Type>;
    static constexpr const bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    static inline NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
#if defined (__GNUC__) && __GNUC__ >= 10
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wvector-operation-performance"
#endif
        return -static_cast<ResultType>(a);
#if defined (__GNUC__) && __GNUC__ >= 10
    #pragma GCC diagnostic pop
#endif
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
        return { .is_monotonic = true, .is_positive = false };
    }
};

void registerFunctionNegate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNegate>();
}

}
