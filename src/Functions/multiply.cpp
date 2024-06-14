#include <type_traits>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

template <typename A, typename B>
struct MultiplyImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        {
            using CastA = std::conditional_t<std::is_floating_point_v<B>, B, A>;
            using CastB = std::conditional_t<std::is_floating_point_v<A>, A, B>;

            return static_cast<Result>(static_cast<CastA>(a)) * static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) * b;
    }

    /// Apply operation and check overflow. It's used for Decimal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result & c)
    {
        if constexpr (std::is_same_v<Result, float> || std::is_same_v<Result, double>)
        {
            c = static_cast<Result>(a) * b;
            return false;
        }
        else
            return common::mulOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateMul(left, right) : b.CreateFMul(left, right);
    }
#endif
};

struct NameMultiply { static constexpr auto name = "multiply"; };
using FunctionMultiply = BinaryArithmeticOverloadResolver<MultiplyImpl, NameMultiply>;

REGISTER_FUNCTION(Multiply)
{
    factory.registerFunction<FunctionMultiply>();
}

}
