#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Working with UInt8: last bit = can be true, previous = can be false (Like src/Storages/MergeTree/BoolMask.h).
/// This function wraps bool atomic functions
/// and transforms their boolean return value to the BoolMask ("can be false" and "can be true" bits).
template <typename A>
struct BitWrapperFuncImpl
{
    using ResultType = UInt8;
    static constexpr const bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    static inline ResultType NO_SANITIZE_UNDEFINED apply(A a [[maybe_unused]])
    {
        // Should be a logical error, but this function is callable from SQL.
        // Need to investigate this.
        if constexpr (!is_integer<A>)
            throw DB::Exception("It's a bug! Only integer types are supported by __bitWrapperFunc.", ErrorCodes::BAD_ARGUMENTS);
        return a == 0 ? static_cast<ResultType>(0b10) : static_cast<ResultType >(0b1);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameBitWrapperFunc { static constexpr auto name = "__bitWrapperFunc"; };
using FunctionBitWrapperFunc = FunctionUnaryArithmetic<BitWrapperFuncImpl, NameBitWrapperFunc, true>;

}

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
