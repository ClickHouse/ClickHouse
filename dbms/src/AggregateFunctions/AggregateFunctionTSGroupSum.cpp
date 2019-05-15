#include "AggregateFunctionTSGroupSum.h"
#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"


namespace DB
{
namespace
{
    template <bool rate>
    AggregateFunctionPtr createAggregateFunctionTSgroupSum(const std::string & name, const DataTypes & arguments, const Array & params)
    {
        assertNoParameters(name, params);

        if (arguments.size() < 3)
            throw Exception("Not enough event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<AggregateFunctionTSgroupSum<rate>>(arguments);
    }

}

void registerAggregateFunctionTSgroupSum(AggregateFunctionFactory & factory)
{
    factory.registerFunction("TSgroupSum", createAggregateFunctionTSgroupSum<false>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("TSgroupRateSum", createAggregateFunctionTSgroupSum<true>, AggregateFunctionFactory::CaseInsensitive);
}

}
