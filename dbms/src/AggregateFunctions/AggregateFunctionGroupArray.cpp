#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupArray.h>
#include <DB/AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionGroupArray(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name,
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionGroupArrayNumeric>(*argument_types[0]);

	if (!res)
		res = new AggregateFunctionGroupArrayGeneric;

	return res;
}

}

void registerAggregateFunctionGroupArray(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"groupArray"}, createAggregateFunctionGroupArray);
}

}
