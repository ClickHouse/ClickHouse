#pragma once

#include <Poco/Timespan.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Field.h>


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
		THROW,	/// Кинуть исключение.
		BREAK,	/// Прервать выполнение запроса, вернуть что есть.
		ANY,	/** Только для GROUP BY: не добавлять новые строки в набор,
				  * но продолжать агрегировать для ключей, успевших попасть в набор.
				  */
	};

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
	size_t max_temporary_columns;				// TODO
	size_t max_temporary_non_const_columns;		// TODO

	size_t max_subquery_depth;
	size_t max_pipeline_depth;
	size_t max_expression_depth;				// TODO
	size_t max_expression_elements;				// TODO

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
		max_subquery_depth(100), max_pipeline_depth(1000), max_expression_depth(1000), max_expression_elements(10000),
		readonly(false)
	{
	}

	/// Установить настройку по имени.
	bool trySet(const String & name, const Field & value)
	{
			 if (name == "max_rows_to_read")		max_rows_to_read 		= boost::get<UInt64>(value);
		else if (name == "max_bytes_to_read")		max_bytes_to_read 		= boost::get<UInt64>(value);
		else if (name == "read_overflow_mode")		read_overflow_mode 		= getOverflowMode(boost::get<const String &>(value));

		else if (name == "max_rows_to_group_by")	max_rows_to_group_by 	= boost::get<UInt64>(value);
		else if (name == "group_by_overflow_mode")	group_by_overflow_mode 	= getOverflowModeForGroupBy(boost::get<const String &>(value));

		else if (name == "max_rows_to_sort")		max_rows_to_sort 		= boost::get<UInt64>(value);
		else if (name == "max_bytes_to_sort")		max_bytes_to_sort 		= boost::get<UInt64>(value);
		else if (name == "sort_overflow_mode")		sort_overflow_mode 		= getOverflowMode(boost::get<const String &>(value));

		else if (name == "max_result_rows")			max_result_rows 		= boost::get<UInt64>(value);
		else if (name == "max_result_bytes")		max_result_bytes 		= boost::get<UInt64>(value);
		else if (name == "result_overflow_mode")	result_overflow_mode 	= getOverflowMode(boost::get<const String &>(value));

		else if (name == "max_execution_time")		max_execution_time 		= Poco::Timespan(boost::get<UInt64>(value), 0);
		else if (name == "timeout_overflow_mode")	timeout_overflow_mode 	= getOverflowMode(boost::get<const String &>(value));

		else if (name == "min_execution_speed")		min_execution_speed 	= boost::get<UInt64>(value);
		else if (name == "timeout_before_checking_execution_speed")
			timeout_before_checking_execution_speed 						= Poco::Timespan(boost::get<UInt64>(value), 0);

		else if (name == "max_columns_to_read")		max_columns_to_read 	= boost::get<UInt64>(value);
		else if (name == "max_temporary_columns")	max_temporary_columns 	= boost::get<UInt64>(value);
		else if (name == "max_temporary_non_const_columns")
			max_temporary_non_const_columns 								= boost::get<UInt64>(value);

		else if (name == "max_subquery_depth")		max_subquery_depth 		= boost::get<UInt64>(value);
		else if (name == "max_pipeline_depth")		max_pipeline_depth 		= boost::get<UInt64>(value);
		else if (name == "max_expression_depth")	max_expression_depth 	= boost::get<UInt64>(value);
		else if (name == "max_expression_elements")	max_expression_elements = boost::get<UInt64>(value);

		else if (name == "readonly")				readonly 				= boost::get<UInt64>(value);
		else
			return false;

		return true;
	}

private:
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
