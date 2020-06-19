#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMannWhitney.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"

#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>


// the return type is boolean (we use UInt8 as we do not have boolean in clickhouse)

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
}

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionMannWhitney(const std::string & name,
                                                       const DataTypes & argument_types,
                                                       const Array & parameters)
{
    assertBinary(name, argument_types);

    // default value
    Float64 significance_level = 0.01;
    if (parameters.size() > 1)
    {
        throw Exception("Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    if (!parameters.empty())
    {
        significance_level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    }

    AggregateFunctionPtr res;

    if (isDecimal(argument_types[0]))
    {
        throw Exception("Aggregate function " + name + " only supports numerical types.", ErrorCodes::NOT_IMPLEMENTED);
    }

    else
    {
        //TO DO check if argument_types[0] == argument_types[1]
        res.reset(createWithTwoNumericTypes<AggregateFunctionMannWhitney>(*argument_types[0], *argument_types[1], significance_level,
                                                                         argument_types, parameters));
    }


    if (!res)
    {
        throw Exception("Aggregate function " + name + " only supports numerical types.", ErrorCodes::NOT_IMPLEMENTED);
    }

    return res;
}

}


void registerAggregateFunctionMannWhitney(AggregateFunctionFactory & factory)
{
    factory.registerFunction("MannWhitney", createAggregateFunctionMannWhitney, AggregateFunctionFactory::CaseInsensitive);
}

}