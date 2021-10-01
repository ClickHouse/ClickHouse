#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{

template <typename A>
struct AbsImpl
{
    using ResultType = std::conditional_t<is_decimal<A>, A, typename NumberTraits::ResultOfAbs<A>::Type>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    static inline NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        if constexpr (is_decimal<A>)
            return a < A(0) ? A(-a) : a;
        else if constexpr (is_big_int_v<A> && is_signed_v<A>)
            return (a < 0) ? -a : a;
        else if constexpr (is_integer<A> && is_signed_v<A>)
            return a < 0 ? static_cast<ResultType>(~a) + 1 : static_cast<ResultType>(a);
        else if constexpr (is_integer<A> && is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(std::abs(a));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// special type handling, some other time
#endif
};

struct NameAbs { static constexpr auto name = "abs"; };
using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;

template <> struct FunctionUnaryArithmeticMonotonicity<NameAbs>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if ((left_float < 0 && right_float > 0) || (left_float > 0 && right_float < 0))
            return {};

        return { .is_monotonic = true, .is_positive = left_float > 0 };
    }
};

void registerFunctionAbs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAbs>(FunctionFactory::CaseInsensitive);
}

}
