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
		THROW 	= 0,	/// Кинуть исключение.
		BREAK 	= 1,	/// Прервать выполнение запроса, вернуть что есть.
		ANY		= 2,	/** Только для GROUP BY: не добавлять новые строки в набор,
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
	bool trySet(const String & name, const Field & value);

	/// Установить настройку по имени. Прочитать сериализованное в бинарном виде значение из буфера (для межсерверного взаимодействия).
	bool trySet(const String & name, ReadBuffer & buf);

	/// Установить настройку по имени. Прочитать значение в текстовом виде из строки (например, из конфига, или из параметра URL).
	bool trySet(const String & name, const String & value);

private:
	friend class Settings;
	
	/// Записать все настройки в буфер. (В отличие от соответствующего метода в Settings, пустая строка на конце не пишется).
	void serialize(WriteBuffer & buf) const;
};


}
