#include "AggregateFunctionTimeSeriesGroupSum.h"
#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"
#include "registerAggregateFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
namespace
{
    template <bool rate>
    AggregateFunctionPtr createAggregateFunctionTimeSeriesGroupSum(const std::string & name, const DataTypes & arguments, const Array & params)
    {
        assertNoParameters(name, params);

        if (arguments.size() < 3)
            throw Exception("Not enough event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<AggregateFunctionTimeSeriesGroupSum<rate>>(arguments);
    }

}

void registerAggregateFunctionTimeSeriesGroupSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesGroupSum", createAggregateFunctionTimeSeriesGroupSum<false>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("timeSeriesGroupRateSum", createAggregateFunctionTimeSeriesGroupSum<true>, AggregateFunctionFactory::CaseInsensitive);
}

}
