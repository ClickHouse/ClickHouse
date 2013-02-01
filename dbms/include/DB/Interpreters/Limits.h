#pragma once

#include <Poco/Timespan.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Field.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/** Ограничения при выполнении запроса - часть настроек.
  * Используются, чтобы обеспечить более безопасное исполнение запросов из пользовательского интерфейса.
  * В основном, ограничения проверяются на каждый блок (а не на каждую строку). То есть, ограничения могут быть немного нарушены.
  * Почти все ограничения действуют только на SELECT-ы.
  * Почти все ограничения действуют на каждый поток по отдельности.
  */
struct Limits
{
	/// Что делать, если ограничение превышено.
	enum OverflowMode
	{
		THROW 	= 0,	/// Кинуть исключение.
		BREAK 	= 1,	/// Прервать выполнение запроса, вернуть что есть.
		ANY		= 2,	/** Только для GROUP BY: не добавлять новые строки в набор,
						  * но продолжать агрегировать для ключей, успевших попасть в набор.
						  */
	};

	static String toString(OverflowMode mode)
	{
		const char * strings[] = { "throw", "break", "any" };

		if (mode < THROW || mode > ANY)
			throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

		return strings[mode];
	}

	/** Ограничения на чтение из самых "глубоких" источников.
	  * То есть, только в самом глубоком подзапросе.
	  * При чтении с удалённого сервера, проверяется только на удалённом сервере.
	  */
	size_t max_rows_to_read;
	size_t max_bytes_to_read;
	OverflowMode read_overflow_mode;

	size_t max_rows_to_group_by;
	OverflowMode group_by_overflow_mode;
	
	size_t max_rows_to_sort;
	size_t max_bytes_to_sort;
	OverflowMode sort_overflow_mode;

	/** Ограничение на размер результата.
	  * Проверяются также для подзапросов и на удалённых серверах.
	  */
	size_t max_result_rows;
	size_t max_result_bytes;
	OverflowMode result_overflow_mode;

	Poco::Timespan max_execution_time;	// TODO: Проверять также при merge стадии сортировки, при слиянии и финализации агрегатных функций.
	OverflowMode timeout_overflow_mode;

	size_t min_execution_speed;								/// В строчках в секунду.
	Poco::Timespan timeout_before_checking_execution_speed;	/// Проверять, что скорость не слишком низкая, после прошествия указанного времени.

	size_t max_columns_to_read;
	size_t max_temporary_columns;
	size_t max_temporary_non_const_columns;

	size_t max_subquery_depth;
	size_t max_pipeline_depth;
	size_t max_ast_depth;				/// Проверяются не во время парсинга, 
	size_t max_ast_elements;			///  а уже после парсинга запроса. TODO: циклы при разборе алиасов в Expression.

	bool readonly;
	
	/// По-умолчанию: всё не ограничено, кроме довольно слабых ограничений на глубину рекурсии и размер выражений.
	Limits() :
		max_rows_to_read(0), max_bytes_to_read(0), read_overflow_mode(THROW),
		max_rows_to_group_by(0), group_by_overflow_mode(THROW),
		max_rows_to_sort(0), max_bytes_to_sort(0), sort_overflow_mode(THROW),
		max_result_rows(0), max_result_bytes(0), result_overflow_mode(THROW),
		max_execution_time(0), timeout_overflow_mode(THROW),
		min_execution_speed(0), timeout_before_checking_execution_speed(0),
		max_columns_to_read(0), max_temporary_columns(0), max_temporary_non_const_columns(0),
		max_subquery_depth(100), max_pipeline_depth(1000), max_ast_depth(1000), max_ast_elements(10000),
		readonly(false)
	{
	}

	/// Установить настройку по имени.
	bool trySet(const String & name, const Field & value)
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
		else
			return false;

		return true;
	}

	/// Установить настройку по имени. Прочитать сериализованное значение из буфера.
	bool trySet(const String & name, ReadBuffer & buf)
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
			|| name == "readonly")
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
			|| name == "timeout_overflow_mode")
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

private:
	friend class Settings;
	
	/// Записать все настройки в буфер. (В отличие от соответствующего метода в Settings, пустая строка на конце не пишется).
	void serialize(WriteBuffer & buf) const
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
	}

	OverflowMode getOverflowModeForGroupBy(const String & s)
	{
		if (s == "throw") 	return THROW;
		if (s == "break") 	return BREAK;
		if (s == "any")		return ANY;

		throw Exception("Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
	}
	
	OverflowMode getOverflowMode(const String & s)
	{
		OverflowMode mode = getOverflowModeForGroupBy(s);
		
		if (mode == ANY)
			throw Exception("Illegal overflow mode: 'any' is only for 'group_by_overflow_mode'", ErrorCodes::ILLEGAL_OVERFLOW_MODE);

		return mode;
	}
};


}
