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

template <typename SpecializedFunction>
class Avg2Resolver final : public MidpointResolver<SpecializedFunction>
{
public:
    static constexpr auto name = "avg2";
    static FunctionOverloadResolverPtr create(ContextPtr context_) { return std::make_unique<Avg2Resolver<SpecializedFunction>>(context_); }

    explicit Avg2Resolver(ContextPtr context_)
        : MidpointResolver<SpecializedFunction>(context_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }
};

REGISTER_FUNCTION(Avg2)
{
    FunctionDocumentation::Description description = R"(
Computes and returns the average value of the provided arguments.
Supports numerical and temporal types.
    )";
    FunctionDocumentation::Syntax syntax = "avg2(x1, x2])";
    FunctionDocumentation::Arguments arguments = {{"x1, x2]", "Accepts two values for averaging."}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the average value of the provided arguments, promoted to the largest compatible type."};
    FunctionDocumentation::Examples examples
        = {{"Numeric types",
            R"(
SELECT avg2(toUInt8(3), 1.0) AS result, toTypeName(result) AS type;
-- The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
        )",
            R"(
┌─result─┬─type────┐
│      2 │ Float64 │
└────────┴─────────┘
        )"},
           {"Decimal types",
            R"(
SELECT avg2(toDecimal32(1, 2), 2) AS result, toTypeName(result) AS type;
        )",
            R"(
┌─result─┬─type──────────┐
│    1.5 │ Decimal(9, 2) │
└────────┴───────────────┘
        )"},
           {"Date types",
            R"(
SELECT avg2(toDate('2025-01-01'), toDate('2025-01-05')) AS result, toTypeName(result) AS type;
        )",
            R"(
┌─────result─┬─type─┐
│ 2025-01-03 │ Date │
└────────────┴──────┘
        )"},
           {"DateTime types",
            R"(
SELECT avg2(toDateTime('2025-01-01 00:00:00'), toDateTime('2025-01-03 12:00:00')) AS result, toTypeName(result) AS type;
        )",
            R"(
┌──────────────result─┬─type─────┐
│ 2025-01-02 06:00:00 │ DateTime │
└─────────────────────┴──────────┘
        )"},
           {"Time64 types",
            R"(
SELECT avg2(toTime64('12:00:00', 0), toTime64('14:00:00', 0)) AS result, toTypeName(result) AS type;
        )",
            R"(
┌───result─┬─type──────┐
│ 13:00:00 │ Time64(0) │
└──────────┴───────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<Avg2Resolver<FunctionMidpointBinary>>(documentation, FunctionFactory::Case::Insensitive);
}
}
