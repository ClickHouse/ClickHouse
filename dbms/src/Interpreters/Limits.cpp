#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Interpreters/Limits.h>


namespace DB
{

static String toString(Limits::OverflowMode mode)
{
	const char * strings[] = { "throw", "break", "any" };

	if (mode < Limits::THROW || mode > Limits::ANY)
		throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

	return strings[mode];
}

static Limits::OverflowMode getOverflowModeForGroupBy(const String & s)
{
	if (s == "throw") 	return Limits::THROW;
	if (s == "break") 	return Limits::BREAK;
	if (s == "any")		return Limits::ANY;

	throw Exception("Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
}

static Limits::OverflowMode getOverflowMode(const String & s)
{
	Limits::OverflowMode mode = getOverflowModeForGroupBy(s);

	if (mode == Limits::ANY)
		throw Exception("Illegal overflow mode: 'any' is only for 'group_by_overflow_mode'", ErrorCodes::ILLEGAL_OVERFLOW_MODE);

	return mode;
}


bool Limits::trySet(const String & name, const Field & value)
{
		 if (name == "max_rows_to_read")		max_rows_to_read 		= safeGet<UInt64>(value);
	else if (name == "max_bytes_to_read")		max_bytes_to_read 		= safeGet<UInt64>(value);
	else if (name == "read_overflow_mode")		read_overflow_mode 		= getOverflowMode(safeGet<const String &>(value));

	else if (name == "max_rows_to_group_by")	max_rows_to_group_by 	= safeGet<UInt64>(value);
	else if (name == "group_by_overflow_mode")	group_by_overflow_mode 	= getOverflowModeForGroupBy(safeGet<const String &>(value));

	else if (name == "max_rows_to_sort")		max_rows_to_sort 		= safeGet<UInt64>(value);
	else if (name == "max_bytes_to_sort")		max_bytes_to_sort 		= safeGet<UInt64>(value);
	else if (name == "sort_overflow_mode")		sort_overflow_mode 		= getOverflowMode(safeGet<const String &>(value));

	else if (name == "max_result_rows")			max_result_rows 		= safeGet<UInt64>(value);
	else if (name == "max_result_bytes")		max_result_bytes 		= safeGet<UInt64>(value);
	else if (name == "result_overflow_mode")	result_overflow_mode 	= getOverflowMode(safeGet<const String &>(value));

	else if (name == "max_execution_time")		max_execution_time 		= Poco::Timespan(safeGet<UInt64>(value), 0);
	else if (name == "timeout_overflow_mode")	timeout_overflow_mode 	= getOverflowMode(safeGet<const String &>(value));

	else if (name == "min_execution_speed")		min_execution_speed 	= safeGet<UInt64>(value);
	else if (name == "timeout_before_checking_execution_speed")
		timeout_before_checking_execution_speed 						= Poco::Timespan(safeGet<UInt64>(value), 0);

	else if (name == "max_columns_to_read")		max_columns_to_read 	= safeGet<UInt64>(value);
	else if (name == "max_temporary_columns")	max_temporary_columns 	= safeGet<UInt64>(value);
	else if (name == "max_temporary_non_const_columns")
		max_temporary_non_const_columns 								= safeGet<UInt64>(value);

	else if (name == "max_subquery_depth")		max_subquery_depth 		= safeGet<UInt64>(value);
	else if (name == "max_pipeline_depth")		max_pipeline_depth 		= safeGet<UInt64>(value);
	else if (name == "max_ast_depth")			max_ast_depth 			= safeGet<UInt64>(value);
	else if (name == "max_ast_elements")		max_ast_elements 		= safeGet<UInt64>(value);

	else if (name == "readonly")				readonly 				= safeGet<UInt64>(value);
	
	else if (name == "max_rows_in_set")		max_rows_in_set 		= safeGet<UInt64>(value);
	else if (name == "max_bytes_in_set")		max_bytes_in_set 		= safeGet<UInt64>(value);
	else if (name == "set_overflow_mode")		set_overflow_mode	 	= getOverflowMode(safeGet<const String &>(value));
	
	else if (name == "max_rows_in_distinct")	max_rows_in_distinct	= safeGet<UInt64>(value);
	else if (name == "max_bytes_in_distinct")	max_bytes_in_distinct	= safeGet<UInt64>(value);
	else if (name == "distinct_overflow_mode")	distinct_overflow_mode 	= getOverflowMode(safeGet<const String &>(value));
	
	else
		return false;

	return true;
}


bool Limits::trySet(const String & name, ReadBuffer & buf)
{
	if (   name == "max_rows_to_read"
		|| name == "max_bytes_to_read"
		|| name == "max_rows_to_group_by"
		|| name == "max_rows_to_sort"
		|| name == "max_bytes_to_sort"
		|| name == "max_result_rows"
		|| name == "max_result_bytes"
		|| name == "max_execution_time"
		|| name == "min_execution_speed"
		|| name == "timeout_before_checking_execution_speed"
		|| name == "max_columns_to_read"
		|| name == "max_temporary_columns"
		|| name == "max_temporary_non_const_columns"
		|| name == "max_subquery_depth"
		|| name == "max_pipeline_depth"
		|| name == "max_ast_depth"
		|| name == "max_ast_elements"
		|| name == "readonly"
		|| name == "max_rows_in_set"
		|| name == "max_bytes_in_set"
		|| name == "max_rows_in_distinct"
		|| name == "max_bytes_in_distinct")
	{
		UInt64 value = 0;
		readVarUInt(value, buf);

		if (!trySet(name, value))
			throw Exception("Logical error: unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
	}
	else if (name == "read_overflow_mode"
		|| name == "group_by_overflow_mode"
		|| name == "sort_overflow_mode"
		|| name == "result_overflow_mode"
		|| name == "timeout_overflow_mode"
		|| name == "set_overflow_mode"
		|| name == "distinct_overflow_mode")
	{
		String value;
		readBinary(value, buf);

		if (!trySet(name, value))
			throw Exception("Logical error: unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
	}
	else
		return false;

	return true;
}

bool Limits::trySet(const String & name, const String & value)
{
	if (   name == "max_rows_to_read"
		|| name == "max_bytes_to_read"
		|| name == "max_rows_to_group_by"
		|| name == "max_rows_to_sort"
		|| name == "max_bytes_to_sort"
		|| name == "max_result_rows"
		|| name == "max_result_bytes"
		|| name == "max_execution_time"
		|| name == "min_execution_speed"
		|| name == "timeout_before_checking_execution_speed"
		|| name == "max_columns_to_read"
		|| name == "max_temporary_columns"
		|| name == "max_temporary_non_const_columns"
		|| name == "max_subquery_depth"
		|| name == "max_pipeline_depth"
		|| name == "max_ast_depth"
		|| name == "max_ast_elements"
		|| name == "readonly"
		|| name == "max_rows_in_set"
		|| name == "max_bytes_in_set"
		|| name == "max_rows_in_distinct"
		|| name == "max_bytes_in_distinct")
	{
		if (!trySet(name, parse<UInt64>(value)))
			throw Exception("Logical error: unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
	}
	else if (name == "read_overflow_mode"
		|| name == "group_by_overflow_mode"
		|| name == "sort_overflow_mode"
		|| name == "result_overflow_mode"
		|| name == "timeout_overflow_mode"
		|| name == "set_overflow_mode"
		|| name == "distinct_overflow_mode")
	{
		if (!trySet(name, Field(value)))
			throw Exception("Logical error: unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
	}
	else
		return false;

	return true;
}


void Limits::serialize(WriteBuffer & buf) const
{
	writeStringBinary("max_rows_to_read", buf);			writeVarUInt(max_rows_to_read, buf);
	writeStringBinary("max_bytes_to_read", buf);		writeVarUInt(max_bytes_to_read, buf);
	writeStringBinary("read_overflow_mode", buf);		writeStringBinary(toString(read_overflow_mode), buf);
	writeStringBinary("max_rows_to_group_by", buf);		writeVarUInt(max_rows_to_group_by, buf);
	writeStringBinary("group_by_overflow_mode", buf);	writeStringBinary(toString(group_by_overflow_mode), buf);
	writeStringBinary("max_rows_to_sort", buf);			writeVarUInt(max_rows_to_sort, buf);
	writeStringBinary("max_bytes_to_sort", buf);		writeVarUInt(max_bytes_to_sort, buf);
	writeStringBinary("sort_overflow_mode", buf);		writeStringBinary(toString(sort_overflow_mode), buf);
	writeStringBinary("max_result_rows", buf);			writeVarUInt(max_result_rows, buf);
	writeStringBinary("max_result_bytes", buf);			writeVarUInt(max_result_bytes, buf);
	writeStringBinary("max_execution_time", buf);		writeVarUInt(max_execution_time.totalSeconds(), buf);
	writeStringBinary("timeout_overflow_mode", buf);	writeStringBinary(toString(timeout_overflow_mode), buf);
	writeStringBinary("min_execution_speed", buf);		writeVarUInt(min_execution_speed, buf);
	writeStringBinary("timeout_before_checking_execution_speed", buf); writeVarUInt(timeout_before_checking_execution_speed.totalSeconds(), buf);
	writeStringBinary("max_columns_to_read", buf);		writeVarUInt(max_columns_to_read, buf);
	writeStringBinary("max_temporary_columns", buf);	writeVarUInt(max_temporary_columns, buf);
	writeStringBinary("max_temporary_non_const_columns", buf); writeVarUInt(max_temporary_non_const_columns, buf);
	writeStringBinary("max_subquery_depth", buf);		writeVarUInt(max_subquery_depth, buf);
	writeStringBinary("max_pipeline_depth", buf);		writeVarUInt(max_pipeline_depth, buf);
	writeStringBinary("max_ast_depth", buf);			writeVarUInt(max_ast_depth, buf);
	writeStringBinary("max_ast_elements", buf);			writeVarUInt(max_ast_elements, buf);
	writeStringBinary("readonly", buf);					writeVarUInt(readonly, buf);
	writeStringBinary("max_rows_in_set", buf);			writeVarUInt(max_rows_in_set, buf);
	writeStringBinary("max_bytes_in_set", buf);		writeVarUInt(max_bytes_in_set, buf);
	writeStringBinary("set_overflow_mode", buf);		writeStringBinary(toString(set_overflow_mode), buf);
	writeStringBinary("max_rows_in_distinct", buf);	writeVarUInt(max_rows_in_distinct, buf);
	writeStringBinary("max_bytes_in_distinct", buf);	writeVarUInt(max_bytes_in_distinct, buf);
	writeStringBinary("distinct_overflow_mode", buf);	writeStringBinary(toString(distinct_overflow_mode), buf);
}

}
