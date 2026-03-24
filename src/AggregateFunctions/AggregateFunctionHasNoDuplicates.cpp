#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionHasNoDuplicates.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionHasNoDuplicates(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires at least one argument",
            name);

    if (!parameters.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} does not accept parameters",
            name);

    return std::make_shared<AggregateFunctionHasNoDuplicates>(argument_types);
}

}

void registerAggregateFunctionHasNoDuplicates(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Returns 1 if all argument tuples across the aggregated rows are distinct, 0 otherwise.
Supports early termination: stops processing as soon as the first duplicate is found.
Rows containing NULL in any column are never considered duplicates (per SQL standard).
)";
    FunctionDocumentation::Syntax syntax = "allUnique(expr1, expr2, ...)";
    FunctionDocumentation::Arguments arguments = {{"expr", "One or more expressions to check for uniqueness across rows."}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1 (UInt8) if all rows are distinct, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, {}, {}, category};
    factory.registerFunction("allUnique", {createAggregateFunctionHasNoDuplicates, documentation});
}

}
