#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/AccurateComparison.h>
#include <Functions/LeastGreatestGeneric.h>


namespace DB
{

template <typename A, typename B>
struct LeastBaseImpl
{
    using ResultType = NumberTraits::ResultOfLeast<A, B>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        /** gcc 4.9.2 successfully vectorizes a loop from this function. */
        return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
        {
            /// Follows the IEEE-754 semantics for minNum, except for handling of signaling NaNs. This match’s the behavior of libc fmin.
            return b.CreateMinNum(left, right);
        }

        auto * compare_value = is_signed ? b.CreateICmpSLT(left, right) : b.CreateICmpULT(left, right);
        return b.CreateSelect(compare_value, left, right);
    }
#endif
};

template <typename A, typename B>
struct LeastSpecialImpl
{
    using ResultType = std::make_signed_t<A>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::lessOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// ???
#endif
};

template <typename A, typename B>
using LeastImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, LeastBaseImpl<A, B>, LeastSpecialImpl<A, B>>;

struct NameLeast { static constexpr auto name = "least"; };
using FunctionLeast = FunctionBinaryArithmetic<LeastImpl, NameLeast>;

REGISTER_FUNCTION(Least)
{
    factory.registerFunction<LeastGreatestOverloadResolver<LeastGreatest::Least, FunctionLeast>>({}, FunctionFactory::Case::Insensitive);
}

}
