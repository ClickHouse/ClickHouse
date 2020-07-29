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
        return static_cast<Result>(static_cast<UInt16>(a)) + static_cast<Result>(b);
    else if constexpr (std::is_same_v<B, UInt8>)
        return static_cast<Result>(a) + static_cast<UInt16>(b);
    else
        return static_cast<Result>(a) + static_cast<Result>(b);
}

template <typename A, typename B>
struct PlusImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_decimal = true;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            return applyBigInt<Result>(a, b);
        else
            return static_cast<Result>(a) + b;
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result & c)
    {
        return common::addOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right) : b.CreateFAdd(left, right);
    }
#endif
};

struct NamePlus { static constexpr auto name = "plus"; };
using FunctionPlus = FunctionBinaryArithmetic<PlusImpl, NamePlus>;

void registerFunctionPlus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPlus>();
}

}
