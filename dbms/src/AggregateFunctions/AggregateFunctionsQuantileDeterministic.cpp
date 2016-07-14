#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/Helpers.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantileDeterministic.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileDeterministic(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 2)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const auto determinator_type = argument_types[1].get();
	if (!typeid_cast<const DataTypeInt32 *>(determinator_type) &&
		!typeid_cast<const DataTypeUInt32 *>(determinator_type) &&
		!typeid_cast<const DataTypeInt64 *>(determinator_type) &&
		!typeid_cast<const DataTypeUInt64 *>(determinator_type))
	{
		throw Exception{
			"Illegal type " + determinator_type->getName() + " of second argument for aggregate function " + name +
			", Int32, UInt32, Int64 or UInt64 required",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
		};
	}

	const IDataType & argument_type = *argument_types[0];

			if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<UInt8>>();
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<UInt16>>();
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<UInt32>>();
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<UInt64>>();
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<Int8>>();
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<Int16>>();
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<Int32>>();
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<Int64>>();
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<Float32>>();
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantileDeterministic<Float64>>();
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return std::make_shared<AggregateFunctionQuantileDeterministic<DataTypeDate::FieldType, false>>();
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return std::make_shared<AggregateFunctionQuantileDeterministic<DataTypeDateTime::FieldType, false>>();
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


AggregateFunctionPtr createAggregateFunctionQuantilesDeterministic(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 2)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const auto determinator_type = argument_types[1].get();
	if (!typeid_cast<const DataTypeInt32 *>(determinator_type) &&
		!typeid_cast<const DataTypeUInt32 *>(determinator_type) &&
		!typeid_cast<const DataTypeInt64 *>(determinator_type) &&
		!typeid_cast<const DataTypeUInt64 *>(determinator_type))
	{
		throw Exception{
			"Illegal type " + determinator_type->getName() + " of second argument for aggregate function " + name +
			", Int32, UInt32, Int64 or UInt64 required",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
		};
	}

	const IDataType & argument_type = *argument_types[0];

			if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt8>>();
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt16>>();
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt32>>();
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<UInt64>>();
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<Int8>>();
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<Int16>>();
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<Int32>>();
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<Int64>>();
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<Float32>>();
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return std::make_shared<AggregateFunctionQuantilesDeterministic<Float64>>();
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return std::make_shared<AggregateFunctionQuantilesDeterministic<DataTypeDate::FieldType, false>>();
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return std::make_shared<AggregateFunctionQuantilesDeterministic<DataTypeDateTime::FieldType, false>>();
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantileDeterministic(AggregateFunctionFactory & factory)
{
	factory.registerFunction("quantileDeterministic", createAggregateFunctionQuantileDeterministic);
	factory.registerFunction("medianDeterministic", createAggregateFunctionQuantileDeterministic);
	factory.registerFunction("quantilesDeterministic", createAggregateFunctionQuantilesDeterministic);
}

}
