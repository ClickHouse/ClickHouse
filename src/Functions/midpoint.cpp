#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <Functions/midpoint.h>

namespace DB
{

struct NameMidpoint
{
    static constexpr auto name = "midpoint";
};
using FunctionMidpointBinary = FunctionBinaryArithmetic<MidpointImpl, NameMidpoint>;

REGISTER_FUNCTION(Midpoint)
{
    FunctionDocumentation::Description description = R"(
Computes and returns the average value of the provided arguments.
Supports numerical and temporal types.
    )";
    FunctionDocumentation::Syntax syntax = "midpoint(x1[, x2, ...])";
    FunctionDocumentation::Arguments arguments = {{"x1[, x2, ...]", "Accepts a single value or multiple values for averaging."}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the average value of the provided arguments, promoted to the largest compatible type."};
    FunctionDocumentation::Examples examples
        = {{"Numeric types",
            R"(
SELECT midpoint(1, toUInt8(3), 0.5) AS result, toTypeName(result) AS type;
-- The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
        )",
            R"(
┌─result─┬─type────┐
│    1.5 │ Float64 │
└────────┴─────────┘
        )"},
           {"Decimal types",
            R"(
SELECT midpoint(toDecimal32(1.5, 2), toDecimal32(1, 1), 2) AS result, toTypeName(result) AS type;
        )",
            R"(
┌─result─┬─type──────────┐
│    1.5 │ Decimal(9, 2) │
└────────┴───────────────┘
        )"},
           {"Date types",
            R"(
SELECT midpoint(toDate('2025-01-01'), toDate('2025-01-05')) AS result, toTypeName(result) AS type;
        )",
            R"(
┌─────result─┬─type─┐
│ 2025-01-03 │ Date │
└────────────┴──────┘
        )"},
           {"DateTime types",
            R"(
SELECT midpoint(toDateTime('2025-01-01 00:00:00'), toDateTime('2025-01-03 12:00:00')) AS result, toTypeName(result) AS type;
        )",
            R"(
┌──────────────result─┬─type─────┐
│ 2025-01-02 06:00:00 │ DateTime │
└─────────────────────┴──────────┘
        )"},
           {"Time64 types",
            R"(
SELECT midpoint(toTime64('12:00:00', 0), toTime64('14:00:00', 0)) AS result, toTypeName(result) AS type;
        )",
            R"(
┌───result─┬─type──────┐
│ 13:00:00 │ Time64(0) │
└──────────┴───────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<MidpointResolver<FunctionMidpointBinary>>(documentation, FunctionFactory::Case::Insensitive);
}

}
