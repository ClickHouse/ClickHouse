#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionAnalysisOfVariance.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB {

namespace
{

    AggregateFunctionPtr createAggregateFunctionAnalysisOfVariance(const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
    {
        assertBinary(name, arguments);

        if (parameters.size() != 1)
            throw Exception("Aggregate function " + name + " requires one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isNumber(arguments[0]) || !isNumber(arguments[1]))
            throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::BAD_ARGUMENTS);

        return std::make_shared<AggregateFunctionAnalysisOfVariance>(arguments, parameters);
    }

}

void registerAggregateFunctionAnalysisOfVariance(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .is_order_dependent = false };
    factory.registerFunction("anova", {createAggregateFunctionAnalysisOfVariance, properties}, AggregateFunctionFactory::CaseInsensitive);
}

}
