#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{

template <typename A, typename B>
struct BitXorImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) ^ static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitXorImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateXor(left, right);
    }
#endif
};

struct NameBitXor { static constexpr auto name = "bitXor"; };
using FunctionBitXor = FunctionBinaryArithmetic<BitXorImpl, NameBitXor>;

void registerFunctionBitXor(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitXor>();
}

}
