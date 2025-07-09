#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

template <typename A, typename B>
struct MinusImpl
{
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        {
            using CastA = std::conditional_t<std::is_floating_point_v<B>, B, A>;
            using CastB = std::conditional_t<std::is_floating_point_v<A>, A, B>;

            return static_cast<Result>(static_cast<CastA>(a)) - static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) - static_cast<Result>(b);
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static bool apply(A a, B b, Result & c)
    {
        return common::subOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};

struct NameMinus { static constexpr auto name = "minus"; };
using FunctionMinus = BinaryArithmeticOverloadResolver<MinusImpl, NameMinus>;

REGISTER_FUNCTION(Minus)
{
    factory.registerFunction<FunctionMinus>();
}

}
