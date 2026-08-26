#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/QuantileDD.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool float_return> using FuncQuantileDD = AggregateFunctionQuantile<Value, QuantileDD<Value>, NameQuantileDD, void, std::conditional_t<float_return, Float64, void>, false, true>;
template <typename Value, bool float_return> using FuncQuantilesDD = AggregateFunctionQuantile<Value, QuantileDD<Value>, NameQuantilesDD, void, std::conditional_t<float_return, Float64, void>, true, true>;


template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} requires at least one argument", name);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE, true>>(argument_types, params);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileDD(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    FunctionDocumentation::Description description = R"(
Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a sample with relative-error guarantees.
It works by building a [DD](https://www.vldb.org/pvldb/vol12/p2195-masson.pdf).
    )";
    FunctionDocumentation::Syntax syntax = R"(
quantileDD(relative_accuracy, [level])(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Column with numeric data.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"relative_accuracy", "Relative accuracy of the quantile. Possible values are in the range from 0 to 1. The size of the sketch depends on the range of the data and the relative accuracy. The larger the range and the smaller the relative accuracy, the larger the sketch. The rough memory size of the sketch is `log(max_value/min_value)/relative_accuracy`. The recommended value is 0.001 or higher.", {"Float*"}},
        {"level", "Optional. Level of quantile. Possible values are in the range from 0 to 1. Default value: 0.5.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Approximate quantile of the specified level.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Computing quantile with DD sketch",
        R"(
CREATE TABLE example_table (a UInt32, b Float32) ENGINE = Memory;
INSERT INTO example_table VALUES (1, 1.001), (2, 1.002), (3, 1.003), (4, 1.004);

SELECT quantileDD(0.01, 0.75)(a), quantileDD(0.01, 0.75)(b) FROM example_table;
        )",
        R"(
┌─quantileDD(0.01, 0.75)(a)─┬─quantileDD(0.01, 0.75)(b)─┐
│        2.974233423476717  │                      1.01 │
└───────────────────────────┴───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(NameQuantileDD::name, {createAggregateFunctionQuantile<FuncQuantileDD>, documentation});
    factory.registerFunction(NameQuantilesDD::name, { createAggregateFunctionQuantile<FuncQuantilesDD>, {}, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianDD", NameQuantileDD::name);
}

}
