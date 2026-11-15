#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace Setting
{
extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
}

FormatSettingsEnums::DateTimeOverflowBehavior getDateTimeOverflowBehavior(ContextPtr context)
{
    if (context)
        return context->getSettingsRef()[Setting::date_time_overflow_behavior].value;
    return default_date_time_overflow_behavior;
}

template <typename A, typename B>
struct PlusImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;
    static const constexpr bool is_commutative = true;
    /// Addition and subtraction of 256-bit integers are a carry chain over four 64-bit limbs
    /// (`wide::integer::add_sub`), which compiles to `adds`/`adcs` on AArch64 and `add`/`adc` on x86-64. clang 23
    /// vectorizes the row loop of `BinaryOperation::process` with two rows per iteration, interleaved loads and
    /// stores, and the carries rebuilt from vector comparisons, which is up to 1.5 times slower than the scalar
    /// chain (`wide_integer_add_sub`). Other operations on 256-bit integers, such as the bitwise ones, vectorize well.
    static constexpr bool no_vectorize = sizeof(ResultType) > 16;

    /// Documentation-only — accepts numerics (with the usual promotion rule),
    /// Date / DateTime / DateTime64 + Interval, Array of numerics (broadcast),
    /// and decimals with scale arithmetic. The full promotion matrix isn't
    /// expressible in the current DSL.
    static constexpr auto signature_documentation = "(Any, Any) -> Any";

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        {
            using CastA = std::conditional_t<is_floating_point<B>, B, A>;
            using CastB = std::conditional_t<is_floating_point<A>, A, B>;

            return static_cast<Result>(static_cast<CastA>(a)) + static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) + static_cast<Result>(b);
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static bool apply(A a, B b, Result & c)
    {
        return common::addOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right) : b.CreateFAdd(left, right);
    }
#endif
};

struct NamePlus { static constexpr auto name = "plus"; };
using FunctionPlus = BinaryArithmeticOverloadResolver<PlusImpl, NamePlus>;

REGISTER_FUNCTION(Plus)
{
    FunctionDocumentation::Description description = R"(
Calculates the sum of two values `x` and `y`. Alias: `x + y` (operator).
It is possible to add an integer and a date or date with time. The former
operation increments the number of days in the date, the latter operation
increments the number of seconds in the date with time.
It is also possible to add a date and a time. Adding a `Date` and a `Time`
produces a `DateTime`. Adding a `Date` and a `Time64`, or a `Date32` and
a `Time` or `Time64`, produces a `DateTime64`.
Adding a `Time` or `Time64` to a `DateTime` or `DateTime64` applies the time
value as an offset in seconds. `DateTime` plus `Time` produces a `DateTime`,
any combination involving `DateTime64` or `Time64` produces a `DateTime64`
with the maximum scale of the two arguments.
    )";
    FunctionDocumentation::Syntax syntax = "plus(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Left hand operand."};
    FunctionDocumentation::Argument argument2 = {"y", "Right hand operand."};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the sum of x and y"};
    FunctionDocumentation::Example example1 = {"Adding two numbers", "SELECT plus(5,5)", "10"};
    FunctionDocumentation::Example example2 = {"Adding an integer and a date", "SELECT plus(toDate('2025-01-01'),5)", "2025-01-06"};
    FunctionDocumentation::Example example3 = {"Adding a date and time", "SELECT toDate('2025-01-01') + CAST('14:30:25', 'Time')", "2025-01-01 14:30:25"};
    FunctionDocumentation::Examples examples = {example1, example2, example3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPlus>(documentation);
}

}
