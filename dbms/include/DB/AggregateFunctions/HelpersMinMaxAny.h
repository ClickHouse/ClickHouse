#pragma once

#include <DB/AggregateFunctions/AggregateFunctionsArgMinMax.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

/// min, max, any, anyLast
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction * createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 1)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & argument_type = *argument_types[0];

	     if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<UInt8>>>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<UInt16>>>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<UInt32>>>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<UInt64>>>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Int8>>>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Int16>>>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Int32>>>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Int64>>>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Float32>>>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Float64>>>;
	else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
		return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDate::FieldType>>>;
	else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
		return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>>;
	else if (typeid_cast<const DataTypeString*>(&argument_type))
		return new AggregateFunctionTemplate<Data<SingleValueDataString>>;
	else
		return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>>;
}


/// argMin, argMax
template <template <typename> class MinMaxData, typename ResData>
static IAggregateFunction * createAggregateFunctionArgMinMaxSecond(const String & name, const IDataType & val_type)
{
	     if (typeid_cast<const DataTypeUInt8 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<UInt8>>>>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<UInt16>>>>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<UInt32>>>>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<UInt64>>>>;
	else if (typeid_cast<const DataTypeInt8 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Int8>>>>;
	else if (typeid_cast<const DataTypeInt16 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Int16>>>>;
	else if (typeid_cast<const DataTypeInt32 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Int32>>>>;
	else if (typeid_cast<const DataTypeInt64 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Int64>>>>;
	else if (typeid_cast<const DataTypeFloat32  *>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Float32>>>>;
	else if (typeid_cast<const DataTypeFloat64  *>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Float64>>>>;
	else if (typeid_cast<const DataTypeDate 	*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDate::FieldType>>>>;
	else if (typeid_cast<const DataTypeDateTime*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDateTime::FieldType>>>>;
	else if (typeid_cast<const DataTypeString*>(&val_type))
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataString>>>;
	else
		return new AggregateFunctionsArgMinMax<AggregateFunctionsArgMinMaxData<ResData, MinMaxData<SingleValueDataGeneric>>>;
}

template <template <typename> class MinMaxData>
static IAggregateFunction * createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types)
{
	if (argument_types.size() != 2)
		throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	const IDataType & res_type = *argument_types[0];
	const IDataType & val_type = *argument_types[1];

	     if (typeid_cast<const DataTypeUInt8 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<UInt8>>(name, val_type);
	else if (typeid_cast<const DataTypeUInt16 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<UInt16>>(name, val_type);
	else if (typeid_cast<const DataTypeUInt32 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<UInt32>>(name, val_type);
	else if (typeid_cast<const DataTypeUInt64 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<UInt64>>(name, val_type);
	else if (typeid_cast<const DataTypeInt8 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Int8>>(name, val_type);
	else if (typeid_cast<const DataTypeInt16 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Int16>>(name, val_type);
	else if (typeid_cast<const DataTypeInt32 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Int32>>(name, val_type);
	else if (typeid_cast<const DataTypeInt64 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Int64>>(name, val_type);
	else if (typeid_cast<const DataTypeFloat32  *>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Float32>>(name, val_type);
	else if (typeid_cast<const DataTypeFloat64  *>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Float64>>(name, val_type);
	else if (typeid_cast<const DataTypeDate 	*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDate::FieldType>>(name, val_type);
	else if (typeid_cast<const DataTypeDateTime*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDateTime::FieldType>>(name, val_type);
	else if (typeid_cast<const DataTypeString*>(&res_type))
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataString>(name, val_type);
	else
		return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataGeneric>(name, val_type);
}

}
