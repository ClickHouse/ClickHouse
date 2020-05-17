#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionWelchTTest.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"

#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace
{
//template <typename X = Float64, typename Y = Float64, typename Ret = Float64>
IAggregateFunction * createWithExtraTypes(Float64 significance_level, const DataTypes & argument_types, const Array & parameters)
{
    return new AggregateFunctionWelchTTest(significance_level, argument_types, parameters);
}

//template <typename X = Float64, typename Y = Float64, typename Ret = Float64>
AggregateFunctionPtr createAggregateFunctionWelchTTest(const std::string & name,
                                                           const DataTypes & argument_types,
                                                           const Array & parameters)
{
    // default value
    Float64 significance_level = 0.1;
    if (parameters.size() > 1)
        throw Exception("Aggregate function " + name + " requires two parameters or less.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    if (!parameters.empty())
    {
        significance_level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    }

    AggregateFunctionPtr res (createWithExtraTypes(significance_level, argument_types, parameters));
    return res;
}

}

void registerAggregateFunctionWelchTTest(AggregateFunctionFactory & factory)
{

    factory.registerFunction("WelchTTest", createAggregateFunctionWelchTTest);
}

}
