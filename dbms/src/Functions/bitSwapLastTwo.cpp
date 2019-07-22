#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

template <typename A>
struct BitSwapLastTwoImpl
{
    using ResultType = UInt8;

    static inline ResultType NO_SANITIZE_UNDEFINED apply(A a)
    {
        return static_cast<ResultType>(
                ((static_cast<ResultType>(a) & 1) << 1) | ((static_cast<ResultType>(a) >> 1) & 1));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception("__bitSwapLastTwo expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateOr(
                b.CreateShl(b.CreateAnd(arg, 1), 1),
                b.CreateAnd(b.CreateLShr(arg, 1), 1)
                );
    }
#endif
};

struct NameBitSwapLastTwo { static constexpr auto name = "__bitSwapLastTwo"; };
using FunctionBitSwapLastTwo = FunctionUnaryArithmetic<BitSwapLastTwoImpl, NameBitSwapLastTwo, true>;

template <> struct FunctionUnaryArithmeticMonotonicity<NameBitSwapLastTwo>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

void registerFunctionBitSwapLastTwo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitSwapLastTwo>();
}

}
