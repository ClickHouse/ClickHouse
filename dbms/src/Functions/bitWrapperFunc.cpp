#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

    template <typename A>
    struct BitWrapperFuncImpl
    {
        using ResultType = UInt8;

        static inline ResultType NO_SANITIZE_UNDEFINED apply(A a)
        {
            return a == static_cast<UInt8>(0) ? static_cast<ResultType>(0b10) : static_cast<ResultType >(0b1);
        }

#if USE_EMBEDDED_COMPILER
        static constexpr bool compilable = false;

#endif
    };

    struct NameBitWrapperFunc { static constexpr auto name = "__bitWrapperFunc"; };
    using FunctionBitWrapperFunc = FunctionUnaryArithmetic<BitWrapperFuncImpl, NameBitWrapperFunc, true>;

    template <> struct FunctionUnaryArithmeticMonotonicity<NameBitWrapperFunc>
    {
        static bool has() { return false; }
        static IFunction::Monotonicity get(const Field &, const Field &)
        {
            return {};
        }
    };

    void registerFunctionBitWrapperFunc(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionBitWrapperFunc>();
    }

}
