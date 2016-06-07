#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/HelpersMinMaxAny.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types)
{
	return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(name, argument_types);
}

AggregateFunctionPtr createAggregateFunctionAnyLast(const std::string & name, const DataTypes & argument_types)
{
	return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(name, argument_types);
}

AggregateFunctionPtr createAggregateFunctionMin(const std::string & name, const DataTypes & argument_types)
{
	return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types);
}

AggregateFunctionPtr createAggregateFunctionMax(const std::string & name, const DataTypes & argument_types)
{
	return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types);
}

AggregateFunctionPtr createAggregateFunctionArgMin(const std::string & name, const DataTypes & argument_types)
{
	return createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types);
}

AggregateFunctionPtr createAggregateFunctionArgMax(const std::string & name, const DataTypes & argument_types)
{
	return createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types);
}

}

void registerAggregateFunctionsMinMaxAny(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"any"}, createAggregateFunctionAny);
	factory.registerFunction({"anyLast"}, createAggregateFunctionAnyLast);
	factory.registerFunction({"min"}, createAggregateFunctionMin);
	factory.registerFunction({"max"}, createAggregateFunctionMax);
	factory.registerFunction({"argMin"}, createAggregateFunctionArgMin);
	factory.registerFunction({"argMax"}, createAggregateFunctionArgMax);
}

}
