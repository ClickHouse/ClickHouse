#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Result, typename A, typename B> 
inline Result applySpecial(A /*a*/, B /*b*/) {
    throw Exception("Bit rotate is not implemented for big integers", ErrorCodes::LOGICAL_ERROR);
    // if constexpr (std::is_same_v<A, UInt8>)
    //     return (static_cast<Result>(static_cast<UInt16>(a)) << static_cast<Result>(b))
    //         | (static_cast<Result>(static_cast<UInt16>(a)) >> ((sizeof(Result) * 8) - static_cast<Result>(b)));
    // else
    //     return (static_cast<Result>(a) << static_cast<Result>(static_cast<UInt16>(b)))
    //         | (static_cast<Result>(a) >> ((sizeof(Result) * 8) - static_cast<Result>(static_cast<UInt16>(b))));
}

template <typename A, typename B>
struct BitRotateLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static constexpr bool is_special = is_big_int_v<ResultType>; 

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_special) 
            return applySpecial<Result, A, B>(a, b);
        else
            return (static_cast<Result>(a) << static_cast<Result>(b))
                | (static_cast<Result>(a) >> ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitRotateLeftImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        auto * size = llvm::ConstantInt::get(left->getType(), left->getType()->getPrimitiveSizeInBits());
        /// XXX how is this supposed to behave in signed mode?
        return b.CreateOr(b.CreateShl(left, right), b.CreateLShr(left, b.CreateSub(size, right)));
    }
#endif
};

struct NameBitRotateLeft { static constexpr auto name = "bitRotateLeft"; };
using FunctionBitRotateLeft = FunctionBinaryArithmetic<BitRotateLeftImpl, NameBitRotateLeft>;

void registerFunctionBitRotateLeft(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitRotateLeft>();
}

}
