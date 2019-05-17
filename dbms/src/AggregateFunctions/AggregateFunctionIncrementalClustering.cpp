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
        throw Exception("Aggregate function " + name + " requires exactly one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!WhichDataType(argument_types[i]).isFloat64())
            throw Exception("Illegal type " + argument_types[i]->getName() + " of argument "
                            + std::to_string(i) + "for aggregate function " + name,
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    UInt32 clusters_num = applyVisitor(FieldVisitorConvertToNumber<UInt32>(), parameters[0]);

    if (argument_types.size() < 1)
        throw Exception("Aggregate function " + name + " requires at least two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionIncrementalClustering>(clusters_num, argument_types.size(), argument_types, parameters);
}

}

void registerAggregateFunctionIncrementalClustering(AggregateFunctionFactory & factory)
{
    factory.registerFunction("IncrementalClustering", createAggregateFunctionIncrementalClustering);
}

}
