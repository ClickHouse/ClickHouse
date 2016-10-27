#pragma once

#include <DB/Interpreters/SystemLog.h>


namespace DB
{


/** Позволяет логгировать информацию о выполнении запросов:
  * - о начале выполнения запроса;
  * - метрики производительности, после выполнения запроса;
  * - об ошибках при выполнении запроса.
  */

/** Что логгировать.
  */
struct QueryLogElement
{
	enum Type
	{
		QUERY_START = 1,
		QUERY_FINISH = 2,
		EXCEPTION_BEFORE_START = 3,
		EXCEPTION_WHILE_PROCESSING = 4,
	};

	Type type = QUERY_START;

	/// В зависимости от типа, не все поля могут быть заполнены.

	time_t event_time{};
	time_t query_start_time{};
	UInt64 query_duration_ms{};

	UInt64 read_rows{};
	UInt64 read_bytes{};

	UInt64 result_rows{};
	UInt64 result_bytes{};

	UInt64 memory_usage{};

	String query;

	String exception;
	String stack_trace;

	ClientInfo client_info;

	static std::string name() { return "QueryLog"; }

	static Block createBlock();
	void appendToBlock(Block & block) const;
};


/// Instead of typedef - to allow forward declaration.
class QueryLog : public SystemLog<QueryLogElement>
{
	using SystemLog<QueryLogElement>::SystemLog;
};

}
