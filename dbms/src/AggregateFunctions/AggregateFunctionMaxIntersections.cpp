#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMaxIntersections.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{

namespace
{
    AggregateFunctionPtr createAggregateFunctionMaxIntersections(
        AggregateFunctionIntersectionsKind kind,
        const std::string & name, const DataTypes & argument_types, const Array & parameters)
    {
        assertBinary(name, argument_types);
        assertNoParameters(name, parameters);

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionIntersectionsMax>(*argument_types[0], kind, argument_types));
        if (!res)
            throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }
}

void registerAggregateFunctionsMaxIntersections(AggregateFunctionFactory & factory)
{
    factory.registerFunction("maxIntersections", [](const std::string & name, const DataTypes & argument_types, const Array & parameters)
        { return createAggregateFunctionMaxIntersections(AggregateFunctionIntersectionsKind::Count, name, argument_types, parameters); });

    factory.registerFunction("maxIntersectionsPosition", [](const std::string & name, const DataTypes & argument_types, const Array & parameters)
        { return createAggregateFunctionMaxIntersections(AggregateFunctionIntersectionsKind::Position, name, argument_types, parameters); });
}

}
