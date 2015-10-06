#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupArray.h>
#include <DB/AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionGroupArray(const std::string & name, const DataTypes & argument_types)
{
	return new AggregateFunctionGroupArray;
}

}

void registerAggregateFunctionGroupArray(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"groupArray"}, createAggregateFunctionGroupArray);
}

}
