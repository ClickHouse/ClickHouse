#pragma once

#include <thread>
#include <boost/noncopyable.hpp>
#include <Poco/Net/IPAddress.h>
#include <DB/Core/Types.h>
#include <DB/Common/ConcurrentBoundedQueue.h>
#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Позволяет логгировать информацию о выполнении запросов:
  * - о начале выполнения запроса;
  * - метрики производительности, после выполнения запроса;
  * - об ошибках при выполнении запроса.
  *
  * Логгирование производится асинхронно. Данные передаются в очередь, откуда их читает отдельный поток.
  * Этот поток записывает лог в предназначенную для этого таблицу не чаще, чем с заданной периодичностью.
  */

/** Что логгировать.
  * Структура может меняться при изменении версии сервера.
  * Если при первой записи обнаруживается, что имеющаяся таблица с логами имеет неподходящую стрктуру,
  *  то эта таблица переименовывается (откладывается в сторону) и создаётся новая таблица.
  */
struct QueryLogElement
{
	enum Type
	{
		SHUTDOWN = 0,		/// Эта запись имеет служебное значение.
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

	Context::Interface interface = Context::Interface::TCP;
	Context::HTTPMethod http_method = Context::HTTPMethod::UNKNOWN;
	Poco::Net::IPAddress ip_address;
	String user;
	String query_id;
};


#define DBMS_QUERY_LOG_QUEUE_SIZE 1024

class Context;


class QueryLog : private boost::noncopyable
{
public:

	/** Передаётся имя таблицы, в которую писать лог.
	  * Если таблица не существует, то она создаётся с движком MergeTree, с ключём по event_time.
	  * Если таблица существует, то проверяется, подходящая ли у неё структура.
	  * Если структура подходящая, то будет использоваться эта таблица.
	  * Если нет - то существующая таблица переименовывается в такую же, но с добавлением суффикса _N на конце,
	  *  где N - минимальное число, начиная с 1 такое, что таблицы с таким именем ещё нет;
	  *  и создаётся новая таблица, как будто существующей таблицы не было.
	  */
	QueryLog(Context & context_, const String & database_name_, const String & table_name_, size_t flush_interval_milliseconds_);
	~QueryLog();

	/** Добавить запись в лог.
	  * Сохранение в таблицу делается асинхронно, и в случае сбоя, запись может никуда не попасть.
	  */
	void add(const QueryLogElement & element)
	{
		/// Здесь может быть блокировка. Возможно, в случае переполнения очереди, лучше сразу кидать эксепшен. Или даже отказаться от логгирования запроса.
		queue.push(element);
	}

private:
	Context & context;
	const String database_name;
	const String table_name;
	StoragePtr table;
	const size_t flush_interval_milliseconds;

	/// Очередь всё-таки ограничена. Но размер достаточно большой, чтобы не блокироваться во всех нормальных ситуациях.
	ConcurrentBoundedQueue<QueryLogElement> queue {DBMS_QUERY_LOG_QUEUE_SIZE};

	/** Данные, которые были вынуты из очереди. Здесь данные накапливаются, пока не пройдёт достаточное количество времени.
	  * Можно было бы использовать двойную буферизацию, но предполагается,
	  *  что запись в таблицу с логом будет быстрее, чем обработка большой пачки запросов.
	  */
	std::vector<QueryLogElement> data;

	Logger * log {&Logger::get("QueryLog")};

	/** В этом потоке данные вынимаются из queue, складываются в data, а затем вставляются в таблицу.
	  */
	std::thread saving_thread;

	void threadFunction();
	static Block createBlock();
	void flush();
};


}
