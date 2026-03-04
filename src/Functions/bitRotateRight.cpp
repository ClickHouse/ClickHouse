#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename A, typename B>
struct BitRotateRightImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Bit rotate is not implemented for big integers");
        else
            return (static_cast<Result>(a) >> static_cast<Result>(b))
                | (static_cast<Result>(a) << ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitRotateRightImpl expected an integral type");
        auto * size = llvm::ConstantInt::get(left->getType(), left->getType()->getPrimitiveSizeInBits());
        return b.CreateOr(b.CreateLShr(left, right), b.CreateShl(left, b.CreateSub(size, right)));
    }
#endif
};

struct NameBitRotateRight { static constexpr auto name = "bitRotateRight"; };
using FunctionBitRotateRight = BinaryArithmeticOverloadResolver<BitRotateRightImpl, NameBitRotateRight, true, false>;

}

REGISTER_FUNCTION(BitRotateRight)
{
    factory.registerFunction<FunctionBitRotateRight>();
}

}
