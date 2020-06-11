#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionWelchTTest.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"

#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace DB
{

namespace
{
template <typename X = Float64, typename Y = Float64, typename Ret = UInt8>
static IAggregateFunction * createWithExtraTypes(Float64 significance_level, const DataTypes & argument_types, const Array & parameters)
{
    return new AggregateFunctionWelchTTest(significance_level, argument_types, parameters);
}

template <typename X = Float64, typename Y = Float64, typename Ret = UInt8>
AggregateFunctionPtr createAggregateFunctionWelchTTest(const std::string & name,
                                                       const DataTypes & argument_types,
                                                       const Array & parameters)
{
    // default value
    Float64 significance_level = 0.1;
    if (parameters.size() > 1)
        throw Exception("Aggregate function " + name + " requires one parameter or less.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    if (!parameters.empty())
    {
        significance_level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    }

    AggregateFunctionPtr res (createWithExtraTypes(significance_level, argument_types, parameters));
    return res;
}

}

template <typename X = Float64, typename Y = Float64, typename Ret = UInt8>
void registerAggregateFunctionWelchTTest(AggregateFunctionFactory & factory)
{

    factory.registerFunction("WelchTTest", createAggregateFunctionWelchTTest<X, Y, Ret>);
}

}