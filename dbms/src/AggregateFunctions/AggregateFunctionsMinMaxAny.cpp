#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/HelpersMinMaxAny.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types)
{
	return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(name, argument_types));
}

AggregateFunctionPtr createAggregateFunctionAnyLast(const std::string & name, const DataTypes & argument_types)
{
	return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(name, argument_types));
}

AggregateFunctionPtr createAggregateFunctionAnyHeavy(const std::string & name, const DataTypes & argument_types)
{
	return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyHeavyData>(name, argument_types));
}

AggregateFunctionPtr createAggregateFunctionMin(const std::string & name, const DataTypes & argument_types)
{
	return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types));
}

AggregateFunctionPtr createAggregateFunctionMax(const std::string & name, const DataTypes & argument_types)
{
	return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types));
}

AggregateFunctionPtr createAggregateFunctionArgMin(const std::string & name, const DataTypes & argument_types)
{
	return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types));
}

AggregateFunctionPtr createAggregateFunctionArgMax(const std::string & name, const DataTypes & argument_types)
{
	return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types));
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
