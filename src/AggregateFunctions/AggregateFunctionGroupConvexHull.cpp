#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupConvexHull.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/geometryConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct Settings;

void registerAggregateFunctionGroupConvexHull(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupConvexHull",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);

            if (argument_types.size() != 1 && argument_types.size() != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Aggregate function {} requires 1 or 2 arguments, got {}", name, argument_types.size());

            bool correct_geometry = true;
            if (argument_types.size() == 2)
            {
                if (!isUInt8(argument_types[1]))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Second argument (correct_geometry) for aggregate function {} must be UInt8", name);
            }

            return std::make_shared<AggregateFunctionGroupConvexHull<CartesianPoint>>(argument_types, correct_geometry);
        });
}

}
