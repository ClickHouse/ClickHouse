#include <Functions/FunctionsDecimalArithmetics.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
}

namespace
{

struct MultiplyDecimalsImpl
{
    static constexpr auto name = "multiplyDecimal";
    static constexpr auto suitable_for_short_circuit = false;

    template <typename FirstType, typename SecondType>
    static Decimal256
    execute(FirstType a, SecondType b, UInt16 scale_a, UInt16 scale_b, UInt16 result_scale)
    {
        if (a.value == 0 || b.value == 0)
            return Decimal256(0);

        Int256 sign_a = a.value < 0 ? -1 : 1;
        Int256 sign_b = b.value < 0 ? -1 : 1;

        std::vector<UInt8> a_digits = DecimalOpHelpers::toDigits(a.value * sign_a);
        std::vector<UInt8> b_digits = DecimalOpHelpers::toDigits(b.value * sign_b);

        std::vector<UInt8> multiplied = DecimalOpHelpers::multiply(a_digits, b_digits);

        UInt16 product_scale = scale_a + scale_b;
        while (product_scale < result_scale)
        {
            multiplied.push_back(0);
            ++product_scale;
        }

        while (product_scale > result_scale&& !multiplied.empty())
        {
            multiplied.pop_back();
            --product_scale;
        }

        if (multiplied.empty())
            return Decimal256(0);

        if (multiplied.size() > DecimalUtils::max_precision<Decimal256>)
            throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow: result bigger that Decimal256");

        return Decimal256(sign_a * sign_b * DecimalOpHelpers::fromDigits(multiplied));
    }
};

}

REGISTER_FUNCTION(MultiplyDecimals)
{
    FunctionDocumentation::Description description = R"(
Performs multiplication on two decimals. Result value will be of type [Decimal256](/sql-reference/data-types/decimal).
Result scale can be explicitly specified by `result_scale` argument (const Integer in range `[0, 76]`). If not specified, the result scale is the max scale of given arguments.

:::note
These functions work significantly slower than usual `multiply`.
In case you don't really need controlled precision and/or need fast computation, consider using [multiply](#multiply)
:::
    )";
    FunctionDocumentation::Syntax syntax = "multiplyDecimal(a, b[, result_scale])";
    FunctionDocumentation::Argument argument1 = {"a", "First value. Type [Decimal](/sql-reference/data-types/decimal)."};
    FunctionDocumentation::Argument argument2 = {"b", "Second value. Type [Decimal](/sql-reference/data-types/decimal)."};
    FunctionDocumentation::Argument argument3 = {"result_scale", "Scale of result. Type [Int/UInt](/sql-reference/data-types/int-uint)."};
    FunctionDocumentation::Arguments arguments = {argument1, argument2, argument3};
    FunctionDocumentation::ReturnedValue returned_value = "The result of multiplication with the given scale. Type: [Decimal256](/sql-reference/data-types/decimal).";
    FunctionDocumentation::Example example1 = {"", "SELECT multiplyDecimal(toDecimal256(-12, 0), toDecimal32(-2.1, 1), 1)", "25.2"};
    FunctionDocumentation::Example example2 = {"Difference with regular multiplication", "SELECT multiplyDecimal(toDecimal256(-12, 0), toDecimal32(-2.1, 1), 1)", R"(
┌─multiply(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│                                               -26.8609633 │
└───────────────────────────────────────────────────────────┘
┌─multiplyDecimal(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│                                                         -26.8609 │
└──────────────────────────────────────────────────────────────────┘
    )"};
    FunctionDocumentation::Example example3 = {"", R"(
SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    multiplyDecimal(a, b);
SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    a * b;
    )", R"(
┌─────────────a─┬─────────────b─┬─multiplyDecimal(toDecimal64(-12.647987876, 9), toDecimal64(123.967645643, 9))─┐
│ -12.647987876 │ 123.967645643 │                                                               -1567.941279108 │
└───────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────────┘
Received exception from server (version 22.11.1):
Code: 407. DB::Exception: Received from localhost:9000. DB::Exception: Decimal math overflow:
While processing toDecimal64(-12.647987876, 9) AS a, toDecimal64(123.967645643, 9) AS b, a * b. (DECIMAL_OVERFLOW)
    )"};
    FunctionDocumentation::Examples examples = {example1, example2, example3};
    FunctionDocumentation::IntroducedIn introduced_in = {22, 12};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionsDecimalArithmetics<MultiplyDecimalsImpl>>(documentation);
}

}
