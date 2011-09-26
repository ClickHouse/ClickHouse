#include <DB/AggregateFunctions/AggregateFunctionCount.h>
#include <DB/AggregateFunctions/AggregateFunctionSum.h>
#include <DB/AggregateFunctions/AggregateFunctionAvg.h>
#include <DB/AggregateFunctions/AggregateFunctionAny.h>
#include <DB/AggregateFunctions/AggregateFunctionAnyLast.h>
#include <DB/AggregateFunctions/AggregateFunctionUniq.h>
#include <DB/AggregateFunctions/AggregateFunctionsMinMax.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>


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
	else if (name == "sum")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();
		
		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64"
			|| argument_type_name == "VarUInt")
			return new AggregateFunctionSum<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64"
			|| argument_type_name == "VarInt")
			return new AggregateFunctionSum<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionSum<Float64>;
		else
			throw Exception("Illegal type " + argument_type_name + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	else if (name == "avg")
	{
		if (argument_types.size() != 1)
			throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String argument_type_name = argument_types[0]->getName();

		if (argument_type_name == "UInt8" || argument_type_name == "UInt16"
			|| argument_type_name == "UInt32" || argument_type_name == "UInt64"
			|| argument_type_name == "VarUInt")
			return new AggregateFunctionAvg<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64"
			|| argument_type_name == "VarInt")
			return new AggregateFunctionAvg<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionAvg<Float64>;
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
			|| argument_type_name == "VarUInt"
			|| argument_type_name == "Date" || argument_type_name == "DateTime")
			return new AggregateFunctionUniq<UInt64>;
		else if (argument_type_name == "Int8" || argument_type_name == "Int16"
			|| argument_type_name == "Int32" || argument_type_name == "Int64"
			|| argument_type_name == "VarInt")
			return new AggregateFunctionUniq<Int64>;
		else if (argument_type_name == "Float32" || argument_type_name == "Float64")
			return new AggregateFunctionUniq<Float64>;
		else if (argument_type_name == "String" || 0 == argument_type_name.compare(0, strlen("FixedString"), "FixedString"))
			return new AggregateFunctionUniq<String>;
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
	else
		throw Exception("Unknown type id of aggregate function " + type_id, ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION);
}


AggregateFunctionPtr AggregateFunctionFactory::tryGet(const String & name, const DataTypes & argument_types) const
{
	AggregateFunctionPtr res;

	try
	{
		return get(name, argument_types);
	}
	catch (const DB::Exception & e)
	{
		if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
			return NULL;
		else
			throw;
	}
}


}
