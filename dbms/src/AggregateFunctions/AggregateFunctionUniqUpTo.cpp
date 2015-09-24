#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/Helpers.h>
#include <DB/AggregateFunctions/AggregateFunctionUniqUpTo.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqUpTo(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniqUpTo>(*argument_types[0]);

	if (res)
		return res;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionUniqUpTo<DataTypeDate::FieldType>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionUniqUpTo<DataTypeDateTime::FieldType>;
	else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
		return new AggregateFunctionUniqUpTo<String>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"uniqUpTo"}, createAggregateFunctionUniqUpTo);
}

}
