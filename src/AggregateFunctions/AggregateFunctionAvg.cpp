#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
bool allowType(const DataTypePtr& type) noexcept
{
    const WhichDataType t(type);
    return t.isInt() || t.isUInt() || t.isFloat() || t.isDecimal();
}

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr& data_type = argument_types[0];

    if (!allowType(data_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
            data_type->getName(), name);

    AggregateFunctionPtr res;

    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionAvg>(
            *data_type, argument_types, getDecimalScale(*data_type)));
    else
        res.reset(createWithNumericType<AggregateFunctionAvg>(*data_type, argument_types));

    return res;
}
}

void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description_avg = R"(
Calculates the arithmetic mean.
    )";
    FunctionDocumentation::Syntax syntax_avg = R"(
avg(x)
    )";
    FunctionDocumentation::Parameters parameters_avg = {};
    FunctionDocumentation::Arguments arguments_avg = {
        {"x", "Input values.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_avg = {"Returns the arithmetic mean, otherwise returns `NaN` if the input parameter `x` is empty.", {"Float64"}};
    FunctionDocumentation::Examples examples_avg = {
    {
        "Basic usage",
        R"(
SELECT avg(x) FROM VALUES('x Int8', 0, 1, 2, 3, 4, 5);
        )",
        R"(
┌─avg(x)─┐
│    2.5 │
└────────┘
        )"
    },
    {
        "Empty table returns NaN",
        R"(
CREATE TABLE test (t UInt8) ENGINE = Memory;

SELECT avg(t) FROM test;
        )",
        R"(
┌─avg(x)─┐
│    nan │
└────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_avg = {1, 1};
    FunctionDocumentation::Category category_avg = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_avg = {description_avg, syntax_avg, arguments_avg, parameters_avg, returned_value_avg, examples_avg, introduced_in_avg, category_avg};

    factory.registerFunction("avg", {createAggregateFunctionAvg, {}, documentation_avg}, AggregateFunctionFactory::Case::Insensitive);
}
}
