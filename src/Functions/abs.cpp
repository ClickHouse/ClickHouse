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
    static constexpr bool allow_string_or_fixed_string = false;

    static NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        if constexpr (is_decimal<A>)
            return a < A(0) ? A(-a) : a;
        else if constexpr (is_big_int_v<A> && is_signed_v<A>)
            return (a < 0) ? -a : a;
        else if constexpr (is_integer<A> && is_signed_v<A>)
            return a < 0 ? static_cast<ResultType>(~a) + 1 : static_cast<ResultType>(a);
        else if constexpr (is_integer<A> && is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (is_floating_point<A>)
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
    static IFunction::Monotonicity get(const IDataType &, const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if ((left_float < 0 && right_float > 0) || (left_float > 0 && right_float < 0))
            return {};

        return { .is_monotonic = true, .is_positive = std::min(left_float, right_float) >= 0, .is_strict = true, };
    }
};

REGISTER_FUNCTION(Abs)
{
    FunctionDocumentation::Description description = "Calculates the absolute value of `x`. Has no effect if `x` is of an unsigned type. If `x` is of a signed type, it returns an unsigned number.";
    FunctionDocumentation::Syntax syntax = "abs(x)";
    FunctionDocumentation::Arguments argument = {{"x", "Value to get the absolute value of"}};
    FunctionDocumentation::ReturnedValue returned_value = {"The absolute value of `x`"};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT abs(-0.5)", "0.5"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, argument, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionAbs>(documentation, FunctionFactory::Case::Insensitive);
}

}
