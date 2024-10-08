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
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ?
               static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
        {
            /// Follows the IEEE-754 semantics for maxNum except for the handling of signaling NaNs. This matches the behavior of libc fmax.
            return b.CreateMaxNum(left, right);
        }

        auto * compare_value = is_signed ? b.CreateICmpSGT(left, right) : b.CreateICmpUGT(left, right);
        return b.CreateSelect(compare_value, left, right);
    }
#endif
};

template <typename A, typename B>
struct GreatestSpecialImpl
{
    using ResultType = make_unsigned_t<A>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
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

REGISTER_FUNCTION(Greatest)
{
    factory.registerFunction<LeastGreatestOverloadResolver<LeastGreatest::Greatest, FunctionGreatest>>({}, FunctionFactory::Case::Insensitive);
}

}
