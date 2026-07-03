#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>


namespace DB
{
template <typename A>
struct SignImpl
{
    using ResultType = Int8;
    static constexpr bool allow_string_or_fixed_string = false;

    static NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        if constexpr (is_decimal<A> || is_floating_point<A>)
            return a < A(0) ? -1 : a == A(0) ? 0 : 1;
        else if constexpr (is_signed_v<A>)
            return a < 0 ? -1 : a == 0 ? 0 : 1;
        else if constexpr (is_unsigned_v<A>)
            return a == 0 ? 0 : 1;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameSign
{
    static constexpr auto name = "sign";
};
using FunctionSign = FunctionUnaryArithmetic<SignImpl, NameSign, false>;

template <>
struct FunctionUnaryArithmeticMonotonicity<NameSign>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { .is_monotonic = true };
    }
};

REGISTER_FUNCTION(Sign)
{
    FunctionDocumentation::Description description = R"(
Returns the sign of a real number.
)";
    FunctionDocumentation::Syntax syntax = "sign(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Values from -∞ to +∞.", {"(U)Int*", "Decimal*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `-1` for `x < 0`, `0` for `x = 0`, `1` for `x > 0`.", {"Int8"}};
    FunctionDocumentation::Examples examples = {
        {"Sign for zero", "SELECT sign(0)", "0"},
        {"Sign for positive", "SELECT sign(1)", "1"},
        {"Sign for negative", "SELECT sign(-1)", "-1"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSign>(documentation, FunctionFactory::Case::Insensitive);
}

}
