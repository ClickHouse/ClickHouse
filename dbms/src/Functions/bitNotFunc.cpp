#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

    template <typename A>
    struct BitNotFuncImpl
    {
        using ResultType = UInt8;

        static inline ResultType NO_SANITIZE_UNDEFINED apply(A a)
        {
            return a == 0 ? static_cast<ResultType>(0b10) : static_cast<ResultType >(0b1);
        }

#if USE_EMBEDDED_COMPILER
        static constexpr bool compilable = false;

#endif
    };

    struct NameBitNotFunc { static constexpr auto name = "__bitNotFunc"; };
    using FunctionBitNotFunc = FunctionUnaryArithmetic<BitNotFuncImpl, NameBitNotFunc, true>;

    template <> struct FunctionUnaryArithmeticMonotonicity<NameBitNotFunc>
    {
        static bool has() { return false; }
        static IFunction::Monotonicity get(const Field &, const Field &)
        {
            return {};
        }
    };

    void registerFunctionBitNotFunc(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionBitNotFunc>();
    }

}
