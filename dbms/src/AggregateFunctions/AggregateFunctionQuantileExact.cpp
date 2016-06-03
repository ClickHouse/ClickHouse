#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/Helpers.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantileExact.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileExact(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

		 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantileExact<UInt8>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantileExact<UInt16>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantileExact<UInt32>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantileExact<UInt64>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantileExact<Int8>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantileExact<Int16>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantileExact<Int32>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantileExact<Int64>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantileExact<Float32>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantileExact<Float64>;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantileExact<DataTypeDate::FieldType>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantileExact<DataTypeDateTime::FieldType>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


AggregateFunctionPtr createAggregateFunctionQuantilesExact(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

		 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<UInt8>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<UInt16>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<UInt32>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<UInt64>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<Int8>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<Int16>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<Int32>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantilesExact<Int64>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantilesExact<Float32>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantilesExact<Float64>;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantilesExact<DataTypeDate::FieldType>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantilesExact<DataTypeDateTime::FieldType>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantileExact(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"quantileExact", "medianExact"}, createAggregateFunctionQuantileExact);
	factory.registerFunction({"quantilesExact"}, createAggregateFunctionQuantilesExact);
}

}
