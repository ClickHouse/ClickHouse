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
template<template <typename> class AggregateFunctionTemplate>
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

template<template <typename, typename> class AggregateFunctionTemplate, class Data>
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


template<template <typename, typename> class AggregateFunctionTemplate, template <typename> class Data>
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


/// min, max, any, anyLast
template<template <typename> class AggregateFunctionTemplate, template <typename> class Data>
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


AggregateFunctionPtr AggregateFunctionFactory::get(const String & name, const DataTypes & argument_types, int recursion_level) const
{
	if (name == "count")
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
		return new AggregateFunctionArgMin;
	else if (name == "argMax")
		return new AggregateFunctionArgMax;
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
			return new AggregateFunctionUniq<DataTypeDate::FieldType, AggregateFunctionUniqHLL12Data>;
		else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
			return new AggregateFunctionUniq<DataTypeDateTime::FieldType, AggregateFunctionUniqHLL12Data>;
		else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
			return new AggregateFunctionUniq<String, AggregateFunctionUniqHLL12Data>;
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


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name, int recursion_level) const
{
	static const char * names[]
	{
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
		"quantileDeterministic",
		"quantilesDeterministic",
		nullptr
	};

	for (const char ** it = names; *it; ++it)
		if (0 == strcmp(*it, name.data()))
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
