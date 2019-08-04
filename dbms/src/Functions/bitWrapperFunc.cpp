#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

    /// Working with UInt8: last bit = can be true, previous = can be false (Like dbms/src/Storages/MergeTree/BoolMask.h).
    /// This function wraps bool atomic functions
    /// and transforms their boolean return value to the BoolMask ("can be false" and "can be true" bits).
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
