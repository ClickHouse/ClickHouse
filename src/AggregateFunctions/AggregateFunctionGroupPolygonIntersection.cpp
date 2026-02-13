#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupPolygonIntersection.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/geometryConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct Settings;

void registerAggregateFunctionGroupPolygonIntersection(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupPolygonIntersection",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);

            if (argument_types.size() != 1 && argument_types.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires 1 or 2 arguments, got {}", name, argument_types.size());

            bool correct_geometry = true;
            if (argument_types.size() == 2)
            {
                if (!isUInt8(argument_types[1]))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Second argument (correct_geometry) for aggregate function {} must be UInt8", name);
            }

            return std::make_shared<AggregateFunctionGroupPolygonIntersection<CartesianPoint>>(argument_types, correct_geometry);
        });
}

}
