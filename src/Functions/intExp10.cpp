#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Common/FieldVisitors.h>
#include <Common/intExp.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename A>
struct IntExp10Impl
{
    using ResultType = UInt64;
    static constexpr const bool allow_fixed_string = false;

    static inline ResultType apply([[maybe_unused]] A a)
    {
        if constexpr (is_big_int_v<A> || std::is_same_v<A, Decimal256>)
            throw DB::Exception("IntExp10 is not implemented for big integers", ErrorCodes::NOT_IMPLEMENTED);
        else
            return intExp10(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// library function
#endif
};

struct NameIntExp10 { static constexpr auto name = "intExp10"; };
/// Assumed to be injective for the purpose of query optimization, but in fact it is not injective because of possible overflow.
using FunctionIntExp10 = FunctionUnaryArithmetic<IntExp10Impl, NameIntExp10, true>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp10>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull()
            ? -std::numeric_limits<Float64>::infinity()
            : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);

        Float64 right_float = right.isNull()
            ? std::numeric_limits<Float64>::infinity()
            : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 19)
            return {};

        return { true };
    }
};

void registerFunctionIntExp10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp10>();
}

}
