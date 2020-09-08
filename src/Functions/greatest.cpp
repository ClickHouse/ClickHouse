#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/AccurateComparison.h>
#include <Functions/LeastGreatestGeneric.h>


namespace DB
{

template <typename A, typename B>
struct GreatestBaseImpl
{
    using ResultType = NumberTraits::ResultOfGreatest<A, B>;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return bigint_cast<Result>(a) > bigint_cast<Result>(b) ?
               bigint_cast<Result>(a) : bigint_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
            /// XXX maxnum is basically fmax(), it may or may not match whatever apply() does
            /// XXX CreateMaxNum is broken on LLVM 5.0 and 6.0 (generates minnum instead; fixed in 7)
            return b.CreateBinaryIntrinsic(llvm::Intrinsic::maxnum, left, right);
        return b.CreateSelect(is_signed ? b.CreateICmpSGT(left, right) : b.CreateICmpUGT(left, right), left, right);
    }
#endif
};

template <typename A, typename B>
struct GreatestSpecialImpl
{
    using ResultType = make_unsigned_t<A>;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::greaterOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// ???
#endif
};

template <typename A, typename B>
using GreatestImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, GreatestBaseImpl<A, B>, GreatestSpecialImpl<A, B>>;

struct NameGreatest { static constexpr auto name = "greatest"; };
using FunctionGreatest = FunctionBinaryArithmetic<GreatestImpl, NameGreatest>;

void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<LeastGreatestOverloadResolver<LeastGreatest::Greatest, FunctionGreatest>>(FunctionFactory::CaseInsensitive);
}

}
