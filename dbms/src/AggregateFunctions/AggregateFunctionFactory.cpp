#include <DB/AggregateFunctions/AggregateFunctionCount.h>
#include <DB/AggregateFunctions/AggregateFunctionSum.h>
#include <DB/AggregateFunctions/AggregateFunctionAvg.h>
#include <DB/AggregateFunctions/AggregateFunctionsMinMaxAny.h>
#include <DB/AggregateFunctions/AggregateFunctionsArgMinMax.h>
#include <DB/AggregateFunctions/AggregateFunctionUniq.h>
#include <DB/AggregateFunctions/AggregateFunctionUniqUpTo.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupArray.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupUniqArray.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantile.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantileTiming.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantileDeterministic.h>
#include <DB/AggregateFunctions/AggregateFunctionIf.h>
#include <DB/AggregateFunctions/AggregateFunctionArray.h>
#include <DB/AggregateFunctions/AggregateFunctionState.h>
#include <DB/AggregateFunctions/AggregateFunctionMerge.h>
#include <DB/AggregateFunctions/AggregateFunctionDebug.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>

#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>


namespace DB
{


AggregateFunctionFactory::AggregateFunctionFactory()
{
}


/** Создать агрегатную функцию с числовым типом в параметре шаблона, в зависимости от типа аргумента.
  */
template <template <typename> class AggregateFunctionTemplate>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type)
{
	     if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt8>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt16>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt32>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt64>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<Int8>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<Int16>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<Int32>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<Int64>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionTemplate<Float32>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionTemplate<Float64>;
	else
		return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, class Data>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type)
{
	     if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt8, Data>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt16, Data>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt32, Data>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt64, Data>;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<Int8, Data>;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<Int16, Data>;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<Int32, Data>;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<Int64, Data>;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionTemplate<Float32, Data>;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionTemplate<Float64, Data>;
	else
		return nullptr;
}


template <template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type)
{
	     if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt8, Data<UInt8> >;
	else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt16, Data<UInt16> >;
	else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt32, Data<UInt32> >;
	else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<UInt64, Data<UInt64> >;
	else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionTemplate<Int8, Data<Int8> >;
	else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionTemplate<Int16, Data<Int16> >;
	else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionTemplate<Int32, Data<Int32> >;
	else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionTemplate<Int64, Data<Int64> >;
	else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionTemplate<Float32, Data<Float32> >;
	else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionTemplate<Float64, Data<Float64> >;
	else
		return nullptr;
}


/** Для шаблона с двумя аргументами.
  */
template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate>
static IAggregateFunction * createWithTwoNumericTypesSecond(const IDataType & second_type)
{
	     if (typeid_cast<const DataTypeUInt8 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, UInt8>;
	else if (typeid_cast<const DataTypeUInt16 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, UInt16>;
	else if (typeid_cast<const DataTypeUInt32 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, UInt32>;
	else if (typeid_cast<const DataTypeUInt64 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, UInt64>;
	else if (typeid_cast<const DataTypeInt8 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, Int8>;
	else if (typeid_cast<const DataTypeInt16 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, Int16>;
	else if (typeid_cast<const DataTypeInt32 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, Int32>;
	else if (typeid_cast<const DataTypeInt64 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, Int64>;
	else if (typeid_cast<const DataTypeFloat32 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, Float32>;
	else if (typeid_cast<const DataTypeFloat64 	*>(&second_type))	return new AggregateFunctionTemplate<FirstType, Float64>;
	else
		return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate>
static IAggregateFunction * createWithTwoNumericTypes(const IDataType & first_type, const IDataType & second_type)
{
	     if (typeid_cast<const DataTypeUInt8 	*>(&first_type))	return createWithTwoNumericTypesSecond<UInt8, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeUInt16 	*>(&first_type))	return createWithTwoNumericTypesSecond<UInt16, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeUInt32 	*>(&first_type))	return createWithTwoNumericTypesSecond<UInt32, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeUInt64 	*>(&first_type))	return createWithTwoNumericTypesSecond<UInt64, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeInt8 	*>(&first_type))	return createWithTwoNumericTypesSecond<Int8, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeInt16 	*>(&first_type))	return createWithTwoNumericTypesSecond<Int16, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeInt32 	*>(&first_type))	return createWithTwoNumericTypesSecond<Int32, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeInt64 	*>(&first_type))	return createWithTwoNumericTypesSecond<Int64, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeFloat32 	*>(&first_type))	return createWithTwoNumericTypesSecond<Float32, AggregateFunctionTemplate>(second_type);
	else if (typeid_cast<const DataTypeFloat64 	*>(&first_type))	return createWithTwoNumericTypesSecond<Float64, AggregateFunctionTemplate>(second_type);
	else
		return nullptr;
}


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


AggregateFunctionPtr AggregateFunctionFactory::get(const String & name, const DataTypes & argument_types, int recursion_level) const
{
	if (name == "debug")
		return new AggregateFunctionDebug;
	else if (name == "count")
		return new AggregateFunctionCount;
	else if (name == "any")
		return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(name, argument_types);
	else if (name == "anyLast")
		return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(name, argument_types);
	else if (name == "min")
		return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types);
	else if (name == "max")
		return createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types);
	else if (name == "argMin")
		return createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types);
	else if (name == "argMax")
		return createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types);
	else if (name == "groupArray")
		return new AggregateFunctionGroupArray;
	else if (name == "groupUniqArray")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * arr = typeid_cast<const DataTypeArray *>(&*argument_types[0]);

		AggregateFunctionPtr res;

		if (!arr)
			res = createWithNumericType<AggregateFunctionGroupUniqArray>(*argument_types[0]);
		else
			res = createWithNumericType<AggregateFunctionGroupUniqArrays>(*arr->getNestedType());

		if (!res)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return res;
	}
	else if (name == "sum")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionSum>(*argument_types[0]);

		if (!res)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return res;
	}
	else if (name == "avg")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionAvg>(*argument_types[0]);

		if (!res)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return res;
	}
	else if (name == "uniq")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const IDataType & argument_type = *argument_types[0];

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqUniquesHashSetData>(*argument_types[0]);

		if (res)
			return res;
		else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqUniquesHashSetData>;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqUniquesHashSetData>;
		else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
			return new AggregateFunctionUniq<String, AggregateFunctionUniqUniquesHashSetData>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "uniqHLL12")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const IDataType & argument_type = *argument_types[0];

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqHLL12Data>(*argument_types[0]);

		if (res)
			return res;
		else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqHLL12Data<DataTypeDate::FieldType>>;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqHLL12Data<DataTypeDateTime::FieldType>>;
		else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
			return new AggregateFunctionUniq<String, AggregateFunctionUniqHLL12Data<String>>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "uniqExact")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const IDataType & argument_type = *argument_types[0];

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionUniq, AggregateFunctionUniqExactData>(*argument_types[0]);

		if (res)
			return res;
		else if (typeid_cast<const DataTypeDate 	*>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqExactData<DataTypeDate::FieldType> >;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqExactData<DataTypeDateTime::FieldType> >;
		else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
			return new AggregateFunctionUniq<String, AggregateFunctionUniqExactData<String> >;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "uniqUpTo")
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
	else if (name == "median" || name == "quantile")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const IDataType & argument_type = *argument_types[0];

			 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantile<UInt8>;
		else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantile<UInt16>;
		else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantile<UInt32>;
		else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantile<UInt64>;
		else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantile<Int8>;
		else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantile<Int16>;
		else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantile<Int32>;
		else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantile<Int64>;
		else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantile<Float32>;
		else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantile<Float64>;
		else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantile<DataTypeDate::FieldType, false>;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantile<DataTypeDateTime::FieldType, false>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "quantiles")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const IDataType & argument_type = *argument_types[0];

			 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantiles<UInt8>;
		else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantiles<UInt16>;
		else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantiles<UInt32>;
		else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantiles<UInt64>;
		else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantiles<Int8>;
		else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantiles<Int16>;
		else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantiles<Int32>;
		else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantiles<Int64>;
		else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantiles<Float32>;
		else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantiles<Float64>;
		else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantiles<DataTypeDate::FieldType, false>;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantiles<DataTypeDateTime::FieldType, false>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "medianTiming" || name == "quantileTiming")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionQuantileTiming>(*argument_types[0]);

		if (!res)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return res;
	}
	else if (name == "quantilesTiming")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		AggregateFunctionPtr res = createWithNumericType<AggregateFunctionQuantilesTiming>(*argument_types[0]);

		if (!res)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return res;
	}
	else if (name == "medianTimingWeighted" || name == "quantileTimingWeighted")
	{
		if (argument_types.size() != 2)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		AggregateFunctionPtr res = createWithTwoNumericTypes<AggregateFunctionQuantileTimingWeighted>(*argument_types[0], *argument_types[1]);

		if (!res)
			throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
				+ " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return res;
	}
	else if (name == "quantilesTimingWeighted")
	{
		if (argument_types.size() != 2)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		AggregateFunctionPtr res = createWithTwoNumericTypes<AggregateFunctionQuantilesTimingWeighted>(*argument_types[0], *argument_types[1]);

		if (!res)
			throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
				+ " of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return res;
	}
	else if (name == "quantileDeterministic")
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

			 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<UInt8>;
		else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<UInt16>;
		else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<UInt32>;
		else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<UInt64>;
		else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<Int8>;
		else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<Int16>;
		else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<Int32>;
		else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantileDeterministic<Int64>;
		else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantileDeterministic<Float32>;
		else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantileDeterministic<Float64>;
		else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantileDeterministic<DataTypeDate::FieldType, false>;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantileDeterministic<DataTypeDateTime::FieldType, false>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "quantilesDeterministic")
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

			 if (typeid_cast<const DataTypeUInt8 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<UInt8>;
		else if (typeid_cast<const DataTypeUInt16 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<UInt16>;
		else if (typeid_cast<const DataTypeUInt32 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<UInt32>;
		else if (typeid_cast<const DataTypeUInt64 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<UInt64>;
		else if (typeid_cast<const DataTypeInt8 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<Int8>;
		else if (typeid_cast<const DataTypeInt16 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<Int16>;
		else if (typeid_cast<const DataTypeInt32 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<Int32>;
		else if (typeid_cast<const DataTypeInt64 	*>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<Int64>;
		else if (typeid_cast<const DataTypeFloat32 *>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<Float32>;
		else if (typeid_cast<const DataTypeFloat64 *>(&argument_type))	return new AggregateFunctionQuantilesDeterministic<Float64>;
		else if (typeid_cast<const DataTypeDate 	*>(&argument_type)) return new AggregateFunctionQuantilesDeterministic<DataTypeDate::FieldType, false>;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type)) return new AggregateFunctionQuantilesDeterministic<DataTypeDateTime::FieldType, false>;
		else
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (recursion_level == 0 && name.size() > strlen("State") && !(strcmp(name.data() + name.size() - strlen("State"), "State")))
	{
		/// Для агрегатных функций вида aggState, где agg - имя другой агрегатной функции.
		AggregateFunctionPtr nested = get(String(name.data(), name.size() - strlen("State")), argument_types, recursion_level + 1);
		return new AggregateFunctionState(nested);
	}
	else if (recursion_level == 0 && name.size() > strlen("Merge") && !(strcmp(name.data() + name.size() - strlen("Merge"), "Merge")))
	{
		/// Для агрегатных функций вида aggMerge, где agg - имя другой агрегатной функции.
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(&*argument_types[0]);
		if (!function)
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		AggregateFunctionPtr nested = get(String(name.data(), name.size() - strlen("Merge")), function->getArgumentsDataTypes(), recursion_level + 1);

		if (nested->getName() != function->getFunctionName())
			throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new AggregateFunctionMerge(nested);
	}
	else if (recursion_level <= 1 && name.size() >= 3 && name[name.size() - 2] == 'I' && name[name.size() - 1] == 'f')
	{
		if (argument_types.empty())
			throw Exception{
				"Incorrect number of arguments for aggregate function " + name,
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		/// Для агрегатных функций вида aggIf, где agg - имя другой агрегатной функции.
		DataTypes nested_dt = argument_types;
		nested_dt.pop_back();
		AggregateFunctionPtr nested = get(String(name.data(), name.size() - 2), nested_dt, recursion_level + 1);
		return new AggregateFunctionIf(nested);
	}
	else if (recursion_level <= 2 && name.size() > strlen("Array") && !(strcmp(name.data() + name.size() - strlen("Array"), "Array")))
	{
		/// Для агрегатных функций вида aggArray, где agg - имя другой агрегатной функции.
		size_t num_agruments = argument_types.size();

		DataTypes nested_arguments;
		for (size_t i = 0; i < num_agruments; ++i)
		{
			if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(&*argument_types[i]))
				nested_arguments.push_back(array->getNestedType());
			else
				throw Exception("Illegal type " + argument_types[i]->getName() + " of argument #" + toString(i + 1) + " for aggregate function " + name + ". Must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		AggregateFunctionPtr nested = get(String(name.data(), name.size() - strlen("Array")), nested_arguments, recursion_level + 2); /// + 2, чтобы ни один другой модификатор не мог идти перед Array
		return new AggregateFunctionArray(nested);
	}
	else
		throw Exception("Unknown aggregate function " + name, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::tryGet(const String & name, const DataTypes & argument_types) const
{
	return isAggregateFunctionName(name)
		? get(name, argument_types)
		: NULL;
}

const AggregateFunctionFactory::FunctionNames & AggregateFunctionFactory::getFunctionNames() const
{
	static FunctionNames names
	{
		"debug",
		"count",
		"any",
		"anyLast",
		"min",
		"max",
		"argMin",
		"argMax",
		"sum",
		"avg",
		"uniq",
		"uniqHLL12",
		"uniqExact",
		"uniqUpTo",
		"groupArray",
		"groupUniqArray",
		"median",
		"quantile",
		"quantiles",
		"medianTiming",
		"quantileTiming",
		"quantilesTiming",
		"quantileTimingWeighted",
		"quantilesTimingWeighted",
		"medianTimingWeighted",
		"quantileDeterministic",
		"quantilesDeterministic"
	};

	return names;
}

bool AggregateFunctionFactory::isAggregateFunctionName(const String & name, int recursion_level) const
{
	const auto & names = getFunctionNames();
	if (std::find(names.begin(), names.end(), name) != names.end())
		return true;

	/// Для агрегатных функций вида aggState, где agg - имя другой агрегатной функции.
	if (recursion_level <= 0 && name.size() > strlen("State") && !(strcmp(name.data() + name.size() - strlen("State"), "State")))
		return isAggregateFunctionName(String(name.data(), name.size() - strlen("State")), recursion_level + 1);
	/// Для агрегатных функций вида aggMerge, где agg - имя другой агрегатной функции.
	if (recursion_level <= 0 && name.size() > strlen("Merge") && !(strcmp(name.data() + name.size() - strlen("Merge"), "Merge")))
		return isAggregateFunctionName(String(name.data(), name.size() - strlen("Merge")), recursion_level + 1);
	/// Для агрегатных функций вида aggIf, где agg - имя другой агрегатной функции.
	if (recursion_level <= 1 && name.size() >= 3 && name[name.size() - 2] == 'I' && name[name.size() - 1] == 'f')
		return isAggregateFunctionName(String(name.data(), name.size() - 2), recursion_level + 1);
	/// Для агрегатных функций вида aggArray, где agg - имя другой агрегатной функции.
	if (recursion_level <= 2 && name.size() > strlen("Array") && !(strcmp(name.data() + name.size() - strlen("Array"), "Array")))
		return isAggregateFunctionName(String(name.data(), name.size() - strlen("Array")), recursion_level + 2); /// + 2, чтобы ни один другой модификатор не мог идти перед Array

	return false;
}


}
