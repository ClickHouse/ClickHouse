#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>


namespace DB
{

REGISTER_FUNCTION(Round)
{
    {
        FunctionDocumentation::Description description = R"(
Returns the largest rounded number less than or equal to `x`, where the rounded number is a multiple of `1 / 10 * N`, or the nearest number of the appropriate data type if `1 / 10 * N` isn't exact.

Integer arguments may be rounded with a negative `N` argument.
With non-negative `N` the function returns `x`.

If rounding causes an overflow (for example, `floor(-128, -1)`), the result is undefined.
)";
        FunctionDocumentation::Syntax syntax = "floor(x[, N])";
        FunctionDocumentation::Arguments arguments = {
            {"x", "The value to round.", {"Float*", "Decimal*", "(U)Int*"}},
            {"N", "Optional. The number of decimal places to round to. Defaults to zero, which means rounding to an integer. Can be negative.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a rounded number of the same type as `x`.", {"Float*", "Decimal*", "(U)Int*"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            "SELECT floor(123.45, 1) AS rounded",
            R"(
┌─rounded─┐
│   123.4 │
└─────────┘
            )"
        },
        {
            "Negative precision",
            "SELECT floor(123.45, -1)",
            R"(
┌─floor(123.45, -1)─┐
│               120 │
└───────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionFloor>(documentation, FunctionFactory::Case::Insensitive);
    }

    {
        FunctionDocumentation::Description description = R"(
Like [`floor`](#floor) but returns the smallest rounded number greater than or equal to `x`.
If rounding causes an overflow (for example, `ceiling(255, -1)`), the result is undefined.
)";
        FunctionDocumentation::Syntax syntax = "ceiling(x[, N])";
        FunctionDocumentation::Arguments arguments = {
            {"x", "The value to round.", {"Float*", "Decimal*", "(U)Int*"}},
            {"N", "Optional. The number of decimal places to round to. Defaults to zero, which means rounding to an integer. Can be negative.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a rounded number of the same type as `x`.", {"Float*", "Decimal*", "(U)Int*"}};
        FunctionDocumentation::Examples examples = {
            {
                "Basic usage",
                "SELECT ceiling(123.45, 1) AS rounded",
                R"(
┌─rounded─┐
│   123.5 │
└─────────┘
            )"
            },
            {
                "Negative precision",
                "SELECT ceiling(123.45, -1)",
                R"(
┌─ceiling(123.45, -1)─┐
│                 130 │
└─────────────────────┘
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionCeil>(documentation, FunctionFactory::Case::Insensitive);
    }

    {
        FunctionDocumentation::Description description = R"(
Like [`floor`](#floor) but returns the rounded number with the largest absolute value less than or equal to that of `x`.
)";
        FunctionDocumentation::Syntax syntax = "truncate(x[, N])";
        FunctionDocumentation::Arguments arguments = {
            {"x", "The value to round.", {"Float*", "Decimal*", "(U)Int*"}},
            {"N", "Optional. The number of decimal places to round to. Defaults to zero, which means rounding to an integer.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a rounded number of the same type as `x`.", {"Float*", "Decimal*", "(U)Int*"}};
        FunctionDocumentation::Examples examples = {
            {"Basic usage", "SELECT truncate(123.499, 1) AS res;", "┌───res─┐\n│ 123.4 │\n└───────┘"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionTrunc>(documentation, FunctionFactory::Case::Insensitive);
    }

    {
        FunctionDocumentation::Description description = R"(
Rounds a value to a specified number of decimal places `N`.

- If `N > 0`, the function rounds to the right of the decimal point.
- If `N < 0`, the function rounds to the left of the decimal point.
- If `N = 0`, the function rounds to the next integer.

The function returns the nearest number of the specified order.
If the input value has equal distance to two neighboring numbers, the function uses banker's rounding for `Float*` inputs and rounds away from zero for the other number types (`Decimal*`).

If rounding causes an overflow (for example, `round(255, -1)`), the result is undefined.
)";
        FunctionDocumentation::Syntax syntax = "round(x[, N])";
        FunctionDocumentation::Arguments arguments = {
            {"x", "A number to round.", {"Float*", "Decimal*", "(U)Int*"}},
            {"N", "Optional. The number of decimal places to round to. Defaults to `0`.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a rounded number of the same type as `x`.", {"Float*", "Decimal*", "(U)Int*"}};
        FunctionDocumentation::Examples examples = {
        {
            "Float inputs",
            "SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3;",
            R"(
┌───x─┬─round(x)─┐
│   0 │        0 │
│ 0.5 │        0 │
│   1 │        1 │
└─────┴──────────┘
            )"
        },
        {
            "Decimal inputs",
            "SELECT cast(number / 2 AS  Decimal(10,4)) AS x, round(x) FROM system.numbers LIMIT 3;",
            R"(
┌───x─┬─round(x)─┐
│   0 │        0 │
│ 0.5 │        1 │
│   1 │        1 │
└─────┴──────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionRound>(documentation, FunctionFactory::Case::Insensitive);
    }

    {
        FunctionDocumentation::Description description = R"(
Rounds a number to a specified decimal position `N`.
If the rounding number is halfway between two numbers, the function uses a method of rounding called banker's rounding, which is the default rounding method for floating point numbers defined in IEEE 754.

- If `N > 0`, the function rounds to the right of the decimal point
- If `N < 0`, the function rounds to the left of the decimal point
- If `N = 0`, the function rounds to the next integer

:::info Notes
- When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position.
For example: `3.5` rounds up to `4`, `2.5` rounds down to `2`.
- The `round` function performs the same rounding for floating point numbers.
- The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.
- In other cases, the function rounds numbers to the nearest integer.
:::

:::tip Use banker's rounding for summation or subtraction of numbers
Using banker's rounding, you can reduce the effect that rounding numbers has on the results of summing or subtracting these numbers.

For example, sum numbers `1.5, 2.5, 3.5, 4.5` with different rounding:
- No rounding: `1.5 + 2.5 + 3.5 + 4.5 = 12`.
- Banker's rounding: `2 + 2 + 4 + 4 = 12`.
- Rounding to the nearest integer: `2 + 3 + 4 + 5 = 14`.
:::
)";
        FunctionDocumentation::Syntax syntax = "roundBankers(x[, N])";
        FunctionDocumentation::Arguments arguments = {
            {"x", "A number to round.", {"(U)Int*", "Decimal*", "Float*"}},
            {"[, N]", "Optional. The number of decimal places to round to. Defaults to `0`.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a value rounded by the banker's rounding method.", {"(U)Int*", "Decimal*", "Float*"}};
        FunctionDocumentation::Examples examples = {
            {"Basic usage", "SELECT number / 2 AS x, roundBankers(x, 0) AS b FROM system.numbers LIMIT 10", "┌───x─┬─b─┐\n│   0 │ 0 │\n│ 0.5 │ 0 │\n│   1 │ 1 │\n│ 1.5 │ 2 │\n│   2 │ 2 │\n│ 2.5 │ 2 │\n│   3 │ 3 │\n│ 3.5 │ 4 │\n│   4 │ 4 │\n│ 4.5 │ 4 │\n└─────┴───┘"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionRoundBankers>(documentation, FunctionFactory::Case::Sensitive);
    }

    {
        FunctionDocumentation::Description description = R"(
Rounds a number down to an element in the specified array.
If the value is less than the lower bound, the lower bound is returned.
)";
        FunctionDocumentation::Syntax syntax = "roundDown(num, arr)";
        FunctionDocumentation::Arguments arguments = {
            {"num", "A number to round down.", {"(U)Int*", "Decimal*", "Float*"}},
            {"arr", "Array of elements to round `num` down to.", {"Array((U)Int*)", "Array(Float*)"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a number rounded down to an element in `arr`. If the value is less than the lowest bound, the lowest bound is returned.", {"(U)Int*", "Float*"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            "SELECT *, roundDown(*, [3, 4, 5]) FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5)",
            R"(
┌─number─┬─roundDown(number, [3, 4, 5])─┐
│      0 │                            3 │
│      1 │                            3 │
│      2 │                            3 │
│      3 │                            3 │
│      4 │                            4 │
│      5 │                            5 │
└────────┴──────────────────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionRoundDown>(documentation);
    }

    /// Compatibility aliases.
    factory.registerAlias("ceiling", "ceil", FunctionFactory::Case::Insensitive);
    factory.registerAlias("truncate", "trunc", FunctionFactory::Case::Insensitive);
}

}
