#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/Helpers.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantileTDigest.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionQuantileTDigest(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

		 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<UInt8>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<UInt16>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<UInt32>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<UInt64>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<Int8>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<Int16>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<Int32>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantileTDigest<Int64>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantileTDigest<Float32>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantileTDigest<Float64>;
/*	else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantile<DataTypeDate::FieldType, false>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantile<DataTypeDateTime::FieldType, false>;*/
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/*
AggregateFunctionPtr createAggregateFunctionQuantilesTDigest(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

			if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<UInt8>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<UInt16>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<UInt32>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<UInt64>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<Int8>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<Int16>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<Int32>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantilesTDigest<Int64>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantilesTDigest<Float32>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantilesTDigest<Float64>;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantilesTDigest<DataTypeDate::FieldType, false>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantilesTDigest<DataTypeDateTime::FieldType, false>;
	else
		throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}*/

}

void registerAggregateFunctionsQuantileTDigest(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"quantileTDigest", "medianTDigest"}, createAggregateFunctionQuantileTDigest);
//	factory.registerFunction({"quantilesTDigest"}, createAggregateFunctionQuantilesTDigest);
}

}
