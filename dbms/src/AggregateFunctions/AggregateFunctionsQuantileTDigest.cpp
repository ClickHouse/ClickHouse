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
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantileTDigest<DataTypeDate::FieldType, false>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantileTDigest<DataTypeDateTime::FieldType, false>;
	else
		throw Exception("Illegal type " + argument_type.getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

template <typename T, bool returns_float>
AggregateFunctionPtr createAggregateFunctionQuantileTDigestWeightedImpl(const std::string & name, const DataTypes & argument_types)
{
	const IDataType & argument_type = *argument_types[1];

		 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, UInt8, returns_float>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, UInt16, returns_float>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, UInt32, returns_float>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, UInt64, returns_float>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, Int8, returns_float>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, Int16, returns_float>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, Int32, returns_float>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, Int64, returns_float>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, Float32, returns_float>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantileTDigestWeighted<T, Float64, returns_float>;
	else
		throw Exception("Illegal type " + argument_type.getName() + " of second argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

AggregateFunctionPtr createAggregateFunctionQuantileTDigestWeighted(const std::string & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 2)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

		 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<UInt8, true>(name, argument_types);
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<UInt16, true>(name, argument_types);
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<UInt32, true>(name, argument_types);
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<UInt64, true>(name, argument_types);
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<Int8, true>(name, argument_types);
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<Int16, true>(name, argument_types);
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<Int32, true>(name, argument_types);
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<Int64, true>(name, argument_types);
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<Float32, true>(name, argument_types);
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<Float64, true>(name, argument_types);
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<DataTypeDate::FieldType, false>(name, argument_types);
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return createAggregateFunctionQuantileTDigestWeightedImpl<DataTypeDateTime::FieldType, false>(name, argument_types);
	else
		throw Exception("Illegal type " + argument_type.getName() + " of first argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
	factory.registerFunction({"quantileTDigestWeighted", "medianTDigestWeighted"}, createAggregateFunctionQuantileTDigestWeighted);
//	factory.registerFunction({"quantilesTDigest"}, createAggregateFunctionQuantilesTDigest);
}

}
