#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{
namespace
{

/// Returns 1 if and only if the argument is constant expression.
/// This function exists for development, debugging and demonstration purposes.
class FunctionIsConstant : public IFunction
{
public:
    static constexpr auto name = "isConstant";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIsConstant>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & elem = arguments[0];
        return ColumnUInt8::create(input_rows_count, isColumnConst(*elem.column));
    }
};

}

REGISTER_FUNCTION(IsConstant)
{
    FunctionDocumentation::Description description = R"(
Returns whether the argument is a constant expression.
A constant expression is an expression whose result is known during query analysis, i.e. before execution.
For example, expressions over [literals](/sql-reference/syntax#literals) are constant expressions.
This function is mostly intended for development, debugging and demonstration.
    )";
    FunctionDocumentation::Syntax syntax = "isConstant(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "An expression to check.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `x` is constant, `0` if `x` is non-constant.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Constant expression",
        R"(
SELECT isConstant(x + 1)
FROM (SELECT 43 AS x)
        )",
        R"(
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
        )"
    },
    {
        "Constant with function",
        R"(
WITH 3.14 AS pi
SELECT isConstant(cos(pi))
        )",
        R"(
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
        )"
    },
    {
        "Non-constant expression",
        R"(
SELECT isConstant(number)
FROM numbers(1)
        )",
        R"(
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
        )"
    },
    {
        "Behavior of the now() function",
        R"(
SELECT isConstant(now())
        )",
        R"(
┌─isConstant(now())─┐
│                 1 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsConstant>(documentation);
}

}

