#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGraphHeight.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename Trait, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGraphHeightImpl(const DataTypePtr & argument_type, const Array & parameters, TArgs ... args)
{
    return std::make_shared<GraphHeightGeneralImpl>(argument_type, parameters, std::forward<TArgs>(args)...);
}


AggregateFunctionPtr createAggregateFunctionGraphHeight(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    if (argument_types[0] != argument_types[1]) {
        throw Exception("Parameters for aggregate function " + name + " should be of equal types", ErrorCodes::BAD_ARGUMENTS);
    }
    return std::make_shared<GraphHeightGeneralImpl>(argument_types[0], parameters);
}

}

void registerAggregateFunctionGraphHeight(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = false };

    factory.registerFunction("graphHeight", { createAggregateFunctionGraphHeight, properties });
}

}
