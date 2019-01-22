#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumMap.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace
{

using SumMapArgs = std::pair<const DataTypePtr &, DataTypes>;

SumMapArgs parseArguments(const std::string & name, const DataTypes & arguments)
{
    if (arguments.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments of Array type.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception("First argument for function " + name + " must be an array.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


    const DataTypePtr & keys_type = array_type->getNestedType();

    DataTypes values_types;
    for (size_t i = 1; i < arguments.size(); ++i)
    {
        array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
        if (!array_type)
            throw Exception("Argument #" + toString(i) + " for function " + name + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        values_types.push_back(array_type->getNestedType());
    }

    return  {keys_type, std::move(values_types)};
}

AggregateFunctionPtr createAggregateFunctionSumMap(const std::string & name, const DataTypes & arguments, const Array & params)
{
    assertNoParameters(name, params);

    auto [keys_type, values_types] = parseArguments(name, arguments);

    AggregateFunctionPtr res(createWithNumericBasedType<AggregateFunctionSumMap>(*keys_type, keys_type, values_types));
    if (!res)
        res.reset(createWithDecimalType<AggregateFunctionSumMap>(*keys_type, keys_type, values_types));
    if (!res)
        throw Exception("Illegal type of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionSumMapFiltered(const std::string & name, const DataTypes & arguments, const Array & params)
{
    if (params.size() != 1)
        throw Exception("Aggregate function " + name + "requires exactly one parameter of Array type.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Array keys_to_keep = params.front().safeGet<Array>();

    auto [keys_type, values_types] = parseArguments(name, arguments);

    AggregateFunctionPtr res(createWithNumericBasedType<AggregateFunctionSumMapFiltered>(*keys_type, keys_type, values_types, keys_to_keep));
    if (!res)
        res.reset(createWithDecimalType<AggregateFunctionSumMapFiltered>(*keys_type, keys_type, values_types, keys_to_keep));
    if (!res)
        throw Exception("Illegal type of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}
}

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumMap", createAggregateFunctionSumMap);
    factory.registerFunction("sumMapFiltered", createAggregateFunctionSumMapFiltered);
}

}
