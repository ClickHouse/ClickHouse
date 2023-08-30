#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename A, typename B>
struct BitOrImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static constexpr const bool allow_fixed_string = true;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) | static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitOrImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateOr(left, right);
    }
#endif
};

struct NameBitOr { static constexpr auto name = "bitOr"; };
using FunctionBitOr = BinaryArithmeticOverloadResolver<BitOrImpl, NameBitOr, true, false>;

}

REGISTER_FUNCTION(BitOr)
{
    factory.registerFunction<FunctionBitOr>();
}

}
