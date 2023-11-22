#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>


namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(name, argument_types, parameters, settings));
}

template <bool RespectNulls = false>
AggregateFunctionPtr createAggregateFunctionNullableAny(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(
        createAggregateFunctionSingleNullableValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData, RespectNulls>(
            name, argument_types, parameters, settings));
}

AggregateFunctionPtr createAggregateFunctionAnyLast(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(name, argument_types, parameters, settings));
}

template <bool RespectNulls = false>
AggregateFunctionPtr createAggregateFunctionNullableAnyLast(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleNullableValue<
                                AggregateFunctionsSingleValue,
                                AggregateFunctionAnyLastData,
                                RespectNulls>(name, argument_types, parameters, settings));
}

AggregateFunctionPtr createAggregateFunctionAnyHeavy(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyHeavyData>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionsAny(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("any", { createAggregateFunctionAny, properties });
    factory.registerAlias("any_value", "any", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("first_value", "any", AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("any_respect_nulls", {createAggregateFunctionNullableAny<true>, properties});
    factory.registerAlias("any_value_respect_nulls", "any_respect_nulls", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("first_value_respect_nulls", "any_respect_nulls", AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("anyLast", { createAggregateFunctionAnyLast, properties });
    factory.registerAlias("last_value", "anyLast", AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("anyLast_respect_nulls", {createAggregateFunctionNullableAnyLast<true>, properties});
    factory.registerAlias("last_value_respect_nulls", "anyLast_respect_nulls", AggregateFunctionFactory::CaseInsensitive);

    factory.registerFunction("anyHeavy", {createAggregateFunctionAnyHeavy, properties});
}

}
