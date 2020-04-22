#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
struct BitRotateRightImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        return (static_cast<Result>(a) >> static_cast<Result>(b))
            | (static_cast<Result>(a) << ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitRotateRightImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        auto * size = llvm::ConstantInt::get(left->getType(), left->getType()->getPrimitiveSizeInBits());
        return b.CreateOr(b.CreateLShr(left, right), b.CreateShl(left, b.CreateSub(size, right)));
    }
#endif
};

struct NameBitRotateRight { static constexpr auto name = "bitRotateRight"; };
using FunctionBitRotateRight = FunctionBinaryArithmetic<BitRotateRightImpl, NameBitRotateRight>;

void registerFunctionBitRotateRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitRotateRight>();
}

}
