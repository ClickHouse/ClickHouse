#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
struct BitShiftLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        using CastB = std::conditional_t<std::is_same_v<B, UInt8>, uint8_t, B>;

        if constexpr (is_big_int_v<B>)
            throw Exception("BitShiftLeftImpl is not implemented for big integers as second argument", ErrorCodes::NOT_IMPLEMENTED);
        else if constexpr (is_big_int_v<ResultType>)
            return static_cast<Result>(a) << static_cast<CastB>(b);
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
