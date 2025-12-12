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
    FunctionDocumentation::Description description = R"(
Calculates the arithmetic mean.
    )";
    FunctionDocumentation::Syntax syntax = "avg(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Input values.", {"(U)Int8/16/32/64", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the arithmetic mean, otherwise `NaN` if the input parameter `x` is empty.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
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
┌─avg(t)─┐
│    nan │
└────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction("avg", createAggregateFunctionAvg, documentation, AggregateFunctionFactory::Case::Insensitive);
}
}
