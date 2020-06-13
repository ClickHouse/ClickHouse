#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionWelchTTest.h>
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
//static IAggregateFunction * createWithExtraTypes(Float64 significance_level, const DataTypes & argument_types, const Array & parameters)
//{
//    return new AggregateFunctionWelchTTest<X, Y>(significance_level, argument_types, parameters);
//}

AggregateFunctionPtr createAggregateFunctionWelchTTest(const std::string & name,
                                                       const DataTypes & argument_types,
                                                       const Array & parameters)
{
    // default value
    Float64 significance_level = 0.1;
    if (parameters.size() > 1)
    {
        throw Exception("Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    if (!parameters.empty())
    {
        significance_level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    }

    AggregateFunctionPtr res;

    if (isDecimal(argument_types[0]) || isDecimal(argument_types[1]))
    {
        throw Exception("Aggregate function " + name + " does not support decimal types.", ErrorCodes::NOT_IMPLEMENTED);
    }

    res.reset(createWithTwoNumericTypes<AggregateFunctionWelchTTest>(*argument_types[0], *argument_types[1], significance_level, argument_types, parameters));

    return res;
}

}


void registerAggregateFunctionWelchTTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("WelchTTest", createAggregateFunctionWelchTTest, AggregateFunctionFactory::CaseInsensitive);
}

}
