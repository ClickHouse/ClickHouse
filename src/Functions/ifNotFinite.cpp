#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>


namespace DB
{
namespace
{

/// ifNotFinite(x, y) is equivalent to isFinite(x) ? x : y.
class FunctionIfNotFinite : public IFunction
{
public:
    static constexpr auto name = "ifNotFinite";

    explicit FunctionIfNotFinite(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIfNotFinite>(context);
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto is_finite_type = FunctionFactory::instance().get("isFinite", context)->build({arguments[0]})->getResultType();
        auto if_type = FunctionFactory::instance().get("if", context)->build({{nullptr, is_finite_type, ""}, arguments[0], arguments[1]})->getResultType();
        return if_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName is_finite_columns{arguments[0]};
        auto is_finite = FunctionFactory::instance().get("isFinite", context)->build(is_finite_columns);
        auto res = is_finite->execute(is_finite_columns, is_finite->getResultType(), input_rows_count, /* dry_run = */ false);

        ColumnsWithTypeAndName if_columns
        {
            {res, is_finite->getResultType(), ""},
            arguments[0],
            arguments[1],
        };

        auto func_if = FunctionFactory::instance().get("if", context)->build(if_columns);
        return func_if->execute(if_columns, result_type, input_rows_count, /* dry_run = */ false);
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(IfNotFinite)
{
    FunctionDocumentation::Description description = R"(
Checks whether a floating point value is finite.

You can get a similar result by using the [ternary operator](/sql-reference/functions/conditional-functions#if): `isFinite(x) ? x : y`.
    )";
    FunctionDocumentation::Syntax syntax = "ifNotFinite(x,y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Value to check if infinite.", {"Float*"}},
        {"y", "Fallback value.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
- `x` if `x` is finite.
- `y` if `x` is not finite.
    )"};
    FunctionDocumentation::Examples examples = {{"Usage example","SELECT 1/0 AS infimum, ifNotFinite(infimum,42)","inf  42"}};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIfNotFinite>(documentation);
}

}

