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
struct BitShiftRightImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<B>)
            throw Exception("BitShiftRight is not implemented for big integers as second argument", ErrorCodes::NOT_IMPLEMENTED);
        else if constexpr (is_big_int_v<A>)
            return static_cast<Result>(a) >> static_cast<UInt32>(b);
        else
            return static_cast<Result>(a) >> static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitShiftRightImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return is_signed ? b.CreateAShr(left, right) : b.CreateLShr(left, right);
    }
#endif
};

struct NameBitShiftRight { static constexpr auto name = "bitShiftRight"; };
using FunctionBitShiftRight = BinaryArithmeticOverloadResolver<BitShiftRightImpl, NameBitShiftRight, true, false>;

}

void registerFunctionBitShiftRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftRight>();
}

}
