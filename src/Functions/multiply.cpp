#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <common/arithmeticOverflow.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template <typename Result, typename A, typename B>
inline Result applyBigInt([[maybe_unused]] A a, [[maybe_unused]] B b)
{
    if constexpr (std::is_floating_point_v<A> || std::is_floating_point_v<B>)
        throw Exception("Big floats are not implemented", ErrorCodes::NOT_IMPLEMENTED);
    else if constexpr (std::is_same_v<A, UInt8>)
        return static_cast<Result>(static_cast<UInt16>(a)) * static_cast<Result>(b);
    else if constexpr (std::is_same_v<B, UInt8>)
        return static_cast<Result>(a) * static_cast<UInt16>(b);
    else
        return static_cast<Result>(a) * static_cast<Result>(b);
}

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
            return applyBigInt<Result, A, B>(a, b);
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
