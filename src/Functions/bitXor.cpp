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
struct BitXorImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static constexpr bool allow_fixed_string = true;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) ^ static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitXorImpl expected an integral type");
        return b.CreateXor(left, right);
    }
#endif
};

struct NameBitXor { static constexpr auto name = "bitXor"; };
using FunctionBitXor = BinaryArithmeticOverloadResolver<BitXorImpl, NameBitXor, true, false>;

}

REGISTER_FUNCTION(BitXor)
{
    factory.registerFunction<FunctionBitXor>();
}

}
