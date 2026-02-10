#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupPolygonUnion.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/geometryConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct Settings;

void registerAggregateFunctionGroupPolygonUnion(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupPolygonUnion",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertUnary(name, argument_types);
            return std::make_shared<AggregateFunctionGroupPolygonUnion<CartesianPoint>>(argument_types);
        });
}

}
