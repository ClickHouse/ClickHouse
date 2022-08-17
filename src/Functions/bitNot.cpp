#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;
    static const constexpr bool allow_fixed_string = true;

    static inline ResultType apply(A a)
    {
        return ~static_cast<ResultType>(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception("BitNotImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateNot(arg);
    }
#endif
};

struct NameBitNot { static constexpr auto name = "bitNot"; };
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameBitNot>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

void registerFunctionBitNot(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitNot>();
}

}
