#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/intExp10.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename A>
struct IntExp10Impl
{
    using ResultType = UInt64;
    static constexpr const bool allow_string_or_fixed_string = false;

    static ResultType apply([[maybe_unused]] A a)
    {
        if constexpr (is_big_int_v<A>)
        {
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "IntExp10 is not implemented for big integers");
        }
        else
        {
            if constexpr (std::is_floating_point_v<A>)
            {
                if (std::isnan(a))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "intExp2 must not be called with nan");
                if (a < 0)
                    return 0;
                if (a >= 20)
                    return std::numeric_limits<UInt64>::max();
            }
            return intExp10(static_cast<int>(a));
        }
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

        return { .is_monotonic = true, .is_strict = true };
    }
};

REGISTER_FUNCTION(IntExp10)
{
    factory.registerFunction<FunctionIntExp10>();
}

}
