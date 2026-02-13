#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupConvexHull.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/geometryConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct Settings;

void registerAggregateFunctionGroupConvexHull(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupConvexHull",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);

            if (argument_types.size() != 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Aggregate function {} requires exactly 1 argument, got {}", name, argument_types.size());

            return std::make_shared<AggregateFunctionGroupConvexHull<CartesianPoint>>(argument_types);
        });
}

}
