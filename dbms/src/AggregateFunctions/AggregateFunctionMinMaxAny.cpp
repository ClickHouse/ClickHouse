#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionAnyLast(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionAnyHeavy(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyHeavyData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionMin(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionMax(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionArgMin(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionArgMax(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types, parameters));
}

}

void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory & factory)
{
    factory.registerFunction("any", createAggregateFunctionAny);
    factory.registerFunction("anyLast", createAggregateFunctionAnyLast);
    factory.registerFunction("anyHeavy", createAggregateFunctionAnyHeavy);
    factory.registerFunction("min", createAggregateFunctionMin, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("max", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("argMin", createAggregateFunctionArgMin);
    factory.registerFunction("argMax", createAggregateFunctionArgMax);
}

}
