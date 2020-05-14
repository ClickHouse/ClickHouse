#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Result, typename A, typename B>
inline Result applySpecial([[maybe_unused]] A a, [[maybe_unused]] B b)
{
    if constexpr (is_big_int_v<B>)
        throw Exception("BitRotate is not implemented for big integers as second argument", ErrorCodes::LOGICAL_ERROR);
    else if constexpr (std::is_same_v<B, UInt8>)
        return static_cast<Result>(a) << static_cast<UInt16>(b);
    else
        return static_cast<Result>(a) << b;
}

template <typename A, typename B>
struct BitShiftLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static constexpr bool is_special = is_big_int_v<ResultType>;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_special)
            return applySpecial<Result>(a, b);
        else
            return static_cast<Result>(a) << static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitShiftLeftImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateShl(left, right);
    }
#endif
};

struct NameBitShiftLeft { static constexpr auto name = "bitShiftLeft"; };
using FunctionBitShiftLeft = FunctionBinaryArithmetic<BitShiftLeftImpl, NameBitShiftLeft>;

void registerFunctionBitShiftLeft(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftLeft>();
}

}
