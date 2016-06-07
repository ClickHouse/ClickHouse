#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionSum.h>
#include <DB/AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionSum(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionSum>(*argument_types[0]);

	if (!res)
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return res;
}

}

void registerAggregateFunctionSum(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"sum"}, createAggregateFunctionSum);
}

}
