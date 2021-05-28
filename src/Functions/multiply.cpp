#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <common/arithmeticOverflow.h>

namespace DB
{

template <typename A, typename B>
struct MultiplyImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_decimal = true;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        {
            using CastA = std::conditional_t<std::is_same_v<A, UInt8>, uint8_t, std::conditional_t<std::is_floating_point_v<B>, B, A>>;
            using CastB = std::conditional_t<std::is_same_v<B, UInt8>, uint8_t, std::conditional_t<std::is_floating_point_v<A>, A, B>>;

            return static_cast<Result>(static_cast<CastA>(a)) * static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) * b;
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result & c)
    {
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
using FunctionMultiply = FunctionBinaryArithmetic<MultiplyImpl, NameMultiply>;

void registerFunctionMultiply(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiply>();
}

}
