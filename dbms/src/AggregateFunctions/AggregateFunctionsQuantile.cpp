#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/Helpers.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantile.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

			if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<UInt8>>();
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<UInt16>>();
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<UInt32>>();
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<UInt64>>();
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<Int8>>();
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<Int16>>();
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<Int32>>();
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<Int64>>();
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<Float32>>();
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantile<Float64>>();
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return std::make_shared<AggregateFunctionQuantile<DataTypeDate::FieldType, false>>();
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return std::make_shared<AggregateFunctionQuantile<DataTypeDateTime::FieldType, false>>();
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


AggregateFunctionPtr createAggregateFunctionQuantiles(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

			if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<UInt8>>();
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<UInt16>>();
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<UInt32>>();
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<UInt64>>();
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<Int8>>();
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<Int16>>();
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<Int32>>();
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<Int64>>();
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<Float32>>();
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantiles<Float64>>();
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return std::make_shared<AggregateFunctionQuantiles<DataTypeDate::FieldType, false>>();
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return std::make_shared<AggregateFunctionQuantiles<DataTypeDateTime::FieldType, false>>();
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory)
{
	factory.registerFunction("quantile", createAggregateFunctionQuantile);
	factory.registerFunction("median", createAggregateFunctionQuantile);
	factory.registerFunction("quantiles", createAggregateFunctionQuantiles);
}

}
