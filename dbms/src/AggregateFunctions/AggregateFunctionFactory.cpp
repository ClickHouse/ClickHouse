#include <set>
#include <boost/assign/list_inserter.hpp>

#include <DB/AggregateFunctions/AggregateFunctionCount.h>
#include <DB/AggregateFunctions/AggregateFunctionSum.h>
#include <DB/AggregateFunctions/AggregateFunctionAvg.h>
#include <DB/AggregateFunctions/AggregateFunctionAny.h>
#include <DB/AggregateFunctions/AggregateFunctionAnyLast.h>
#include <DB/AggregateFunctions/AggregateFunctionUniq.h>
#include <DB/AggregateFunctions/AggregateFunctionGroupArray.h>
#include <DB/AggregateFunctions/AggregateFunctionsMinMax.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/AggregateFunctions/AggregateFunctionQuantile.h>

#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>


namespace DB
{


AggregateFunctionFactory::AggregateFunctionFactory()
{
}


AggregateFunctionPtr AggregateFunctionFactory::get(const String & name, const DataTypes & argument_types) const
{
	if (name == "count")
		return new AggregateFunctionCount;
	else if (name == "any")
		return new AggregateFunctionAny;
	else if (name == "anyLast")
		return new AggregateFunctionAnyLast;
	else if (name == "min")
		return new AggregateFunctionMin;
	else if (name == "max")
		return new AggregateFunctionMax;
	else if (name == "groupArray")
		return new AggregateFunctionGroupArray;
	else if (name == "sum")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();
		
		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64")
			return new AggregateFunctionSum<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64")
			return new AggregateFunctionSum<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionSum<Float64>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "sumIf")
	{
		if (argument_types.size() != 2)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();

		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64")
			return new AggregateFunctionSumIf<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64")
			return new AggregateFunctionSumIf<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionSumIf<Float64>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "avg")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();

		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64")
			return new AggregateFunctionAvg<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64")
			return new AggregateFunctionAvg<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionAvg<Float64>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "avgIf")
	{
		if (argument_types.size() != 2)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();

		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64")
			return new AggregateFunctionAvgIf<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64")
			return new AggregateFunctionAvgIf<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionAvgIf<Float64>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "uniq")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();

		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64"
			|| argument_type_name == "Date" || argument_type_name == "DateTime")
			return new AggregateFunctionUniq<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64")
			return new AggregateFunctionUniq<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionUniq<Float64>;
		else if (argument_type_name == "String" || 0 == argument_type_name.compare(0, strlen("FixedString"), "FixedString"))
			return new AggregateFunctionUniq<String>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "uniqIf")
	{
		if (argument_types.size() != 2)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();

		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64"
			|| argument_type_name == "Date" || argument_type_name == "DateTime")
			return new AggregateFunctionUniqIf<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64")
			return new AggregateFunctionUniqIf<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionUniqIf<Float64>;
		else if (argument_type_name == "String" || 0 == argument_type_name.compare(0, strlen("FixedString"), "FixedString"))
			return new AggregateFunctionUniqIf<String>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "uniqState")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();

		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64"
			|| argument_type_name == "Date" || argument_type_name == "DateTime")
			return new AggregateFunctionUniqState<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64")
			return new AggregateFunctionUniqState<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionUniqState<Float64>;
		else if (argument_type_name == "String" || 0 == argument_type_name.compare(0, strlen("FixedString"), "FixedString"))
			return new AggregateFunctionUniqState<String>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "median" || name == "quantile")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		String argument_type_name = argument_types[0]->getName();
		
		if 		(argument_type_name == "UInt8")		return new AggregateFunctionQuantile<UInt8>;
		else if (argument_type_name == "UInt16")	return new AggregateFunctionQuantile<UInt16>;
		else if (argument_type_name == "UInt32")	return new AggregateFunctionQuantile<UInt32>;
		else if (argument_type_name == "UInt64")	return new AggregateFunctionQuantile<UInt64>;
		else if (argument_type_name == "Int8")		return new AggregateFunctionQuantile<Int8>;
		else if (argument_type_name == "Int16")		return new AggregateFunctionQuantile<Int16>;
		else if (argument_type_name == "Int32")		return new AggregateFunctionQuantile<Int32>;
		else if (argument_type_name == "Int64")		return new AggregateFunctionQuantile<Int64>;
		else if (argument_type_name == "Float32")	return new AggregateFunctionQuantile<Float32>;
		else if (argument_type_name == "Float64")	return new AggregateFunctionQuantile<Float64>;
		else if (argument_type_name == "Date")		return new AggregateFunctionQuantile<DataTypeDate::FieldType, false>;
		else if (argument_type_name == "DateTime")	return new AggregateFunctionQuantile<DataTypeDateTime::FieldType, false>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else
		throw Exception("Unknown aggregate function " + name, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::getByTypeID(const String & type_id) const
{
	if (type_id == "count")
		return new AggregateFunctionCount;
	else if (type_id == "any")
		return new AggregateFunctionAny;
	else if (type_id == "anyLast")
		return new AggregateFunctionAnyLast;
	else if (type_id == "min")
		return new AggregateFunctionMin;
	else if (type_id == "max")
		return new AggregateFunctionMax;
	else if (type_id == "groupArray")
		return new AggregateFunctionGroupArray;
	else if (0 == type_id.compare(0, strlen("sum_"), "sum_"))
	{
		if (0 == type_id.compare(strlen("sum_"), strlen("UInt64"), "UInt64"))
			return new AggregateFunctionSum<UInt64>;
		else if (0 == type_id.compare(strlen("sum_"), strlen("Int64"), "Int64"))
			return new AggregateFunctionSum<Int64>;
		else if (0 == type_id.compare(strlen("sum_"), strlen("Float64"), "Float64"))
			return new AggregateFunctionSum<Float64>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("sumIf_"), "sumIf_"))
	{
		if (0 == type_id.compare(strlen("sumIf_"), strlen("UInt64"), "UInt64"))
			return new AggregateFunctionSumIf<UInt64>;
		else if (0 == type_id.compare(strlen("sumIf_"), strlen("Int64"), "Int64"))
			return new AggregateFunctionSumIf<Int64>;
		else if (0 == type_id.compare(strlen("sumIf_"), strlen("Float64"), "Float64"))
			return new AggregateFunctionSumIf<Float64>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("avg_"), "avg_"))
	{
		if (0 == type_id.compare(strlen("avg_"), strlen("UInt64"), "UInt64"))
			return new AggregateFunctionAvg<UInt64>;
		else if (0 == type_id.compare(strlen("avg_"), strlen("Int64"), "Int64"))
			return new AggregateFunctionAvg<Int64>;
		else if (0 == type_id.compare(strlen("avg_"), strlen("Float64"), "Float64"))
			return new AggregateFunctionAvg<Float64>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("avgIf_"), "avgIf_"))
	{
		if (0 == type_id.compare(strlen("avgIf_"), strlen("UInt64"), "UInt64"))
			return new AggregateFunctionAvgIf<UInt64>;
		else if (0 == type_id.compare(strlen("avgIf_"), strlen("Int64"), "Int64"))
			return new AggregateFunctionAvgIf<Int64>;
		else if (0 == type_id.compare(strlen("avgIf_"), strlen("Float64"), "Float64"))
			return new AggregateFunctionAvgIf<Float64>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("uniq_"), "uniq_"))
	{
		if (0 == type_id.compare(strlen("uniq_"), strlen("UInt64"), "UInt64"))
			return new AggregateFunctionUniq<UInt64>;
		else if (0 == type_id.compare(strlen("uniq_"), strlen("Int64"), "Int64"))
			return new AggregateFunctionUniq<Int64>;
		else if (0 == type_id.compare(strlen("uniq_"), strlen("Float64"), "Float64"))
			return new AggregateFunctionUniq<Float64>;
		else if (0 == type_id.compare(strlen("uniq_"), strlen("String"), "String"))
			return new AggregateFunctionUniq<String>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("uniqIf_"), "uniqIf_"))
	{
		if (0 == type_id.compare(strlen("uniqIf_"), strlen("UInt64"), "UInt64"))
			return new AggregateFunctionUniqIf<UInt64>;
		else if (0 == type_id.compare(strlen("uniqIf_"), strlen("Int64"), "Int64"))
			return new AggregateFunctionUniqIf<Int64>;
		else if (0 == type_id.compare(strlen("uniqIf_"), strlen("Float64"), "Float64"))
			return new AggregateFunctionUniqIf<Float64>;
		else if (0 == type_id.compare(strlen("uniqIf_"), strlen("String"), "String"))
			return new AggregateFunctionUniqIf<String>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("uniqState_"), "uniqState_"))
	{
		if (0 == type_id.compare(strlen("uniqState_"), strlen("UInt64"), "UInt64"))
			return new AggregateFunctionUniqState<UInt64>;
		else if (0 == type_id.compare(strlen("uniqState_"), strlen("Int64"), "Int64"))
			return new AggregateFunctionUniqState<Int64>;
		else if (0 == type_id.compare(strlen("uniqState_"), strlen("Float64"), "Float64"))
			return new AggregateFunctionUniqState<Float64>;
		else if (0 == type_id.compare(strlen("uniqState_"), strlen("String"), "String"))
			return new AggregateFunctionUniqState<String>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("quantile_float_"), "quantile_float_"))
	{
		if 		(0 == type_id.compare(strlen("quantile_float_"), strlen("UInt8"), 	"UInt8"))	return new AggregateFunctionQuantile<UInt8>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("UInt16"), 	"UInt16"))	return new AggregateFunctionQuantile<UInt16>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("UInt32"), 	"UInt32"))	return new AggregateFunctionQuantile<UInt32>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("UInt64"), 	"UInt64"))	return new AggregateFunctionQuantile<UInt64>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("Int8"), 	"Int8"))	return new AggregateFunctionQuantile<Int8>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("Int16"), 	"Int16"))	return new AggregateFunctionQuantile<Int16>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("Int32"), 	"Int32"))	return new AggregateFunctionQuantile<Int32>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("Int64"), 	"Int64"))	return new AggregateFunctionQuantile<Int64>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("Float32"), "Float32"))	return new AggregateFunctionQuantile<Float32>;
		else if (0 == type_id.compare(strlen("quantile_float_"), strlen("Float64"), "Float64"))	return new AggregateFunctionQuantile<Float64>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else if (0 == type_id.compare(0, strlen("quantile_rounded_"), "quantile_rounded_"))
	{
		if 		(0 == type_id.compare(strlen("quantile_rounded_"), strlen("UInt8"), 	"UInt8"))	return new AggregateFunctionQuantile<UInt8,		false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("UInt16"), 	"UInt16"))	return new AggregateFunctionQuantile<UInt16,	false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("UInt32"), 	"UInt32"))	return new AggregateFunctionQuantile<UInt32,	false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("UInt64"), 	"UInt64"))	return new AggregateFunctionQuantile<UInt64,	false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("Int8"), 		"Int8"))	return new AggregateFunctionQuantile<Int8,		false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("Int16"), 	"Int16"))	return new AggregateFunctionQuantile<Int16,		false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("Int32"), 	"Int32"))	return new AggregateFunctionQuantile<Int32,		false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("Int64"), 	"Int64"))	return new AggregateFunctionQuantile<Int64,		false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("Float32"), 	"Float32"))	return new AggregateFunctionQuantile<Float32,	false>;
		else if (0 == type_id.compare(strlen("quantile_rounded_"), strlen("Float64"), 	"Float64"))	return new AggregateFunctionQuantile<Float64,	false>;
		else
			throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
	}
	else
		throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::tryGet(const String & name, const DataTypes & argument_types) const
{
	return isAggregateFunctionName(name)
		? get(name, argument_types)
		: NULL;
}


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name) const
{
	std::set<String> names;
	
	boost::assign::insert(names)
		("count")
		("any")
		("anyLast")
		("min")
		("max")
		("sum")
		("sumIf")
		("avg")
		("avgIf")
		("uniq")
		("uniqIf")
		("uniqState")
		("groupArray")
		("median")
		("quantile")
	;

	return names.end() != names.find(name);
}


}
