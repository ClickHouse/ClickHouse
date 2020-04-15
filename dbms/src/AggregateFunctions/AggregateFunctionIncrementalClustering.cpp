#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionIncrementalClustering.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionIncrementalClustering(
    const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (parameters.size() != 1)
        throw Exception("Aggregate function " + name + " requires exactly one parameter - number of clusters",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!isNativeNumber(argument_types[i]))
            throw Exception("Argument " + std::to_string(i) + " of type " + argument_types[i]->getName()
                            + " must be numeric for aggregate function " + name,
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    UInt32 clusters_num = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[0]);

    if (argument_types.empty())
        throw Exception("Aggregate function " + name + " requires at least one arguments",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionIncrementalClustering>(
            clusters_num, argument_types.size(), argument_types, parameters);
}

}

void registerAggregateFunctionIncrementalClustering(AggregateFunctionFactory & factory)
{
    factory.registerFunction("incrementalClustering", createAggregateFunctionIncrementalClustering);
}

}
