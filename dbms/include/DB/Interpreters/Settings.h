#pragma once

#include <Poco/Timespan.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Field.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Interpreters/Limits.h>


namespace DB
{

/** Настройки выполнения запроса.
  */
struct Settings
{
	/// Максимальный размер блока для чтения
	size_t max_block_size;
	/// Максимальное количество потоков выполнения запроса
	size_t max_threads;
	/// Максимальное количество соединений при распределённой обработке одного запроса (должно быть больше, чем max_threads).
	size_t max_distributed_connections;
	/// Какую часть запроса можно прочитать в оперативку для парсинга (оставшиеся данные для INSERT, если есть, считываются позже)
	size_t max_query_size;
	/// Выполнять разные стадии конвейера выполнения запроса параллельно
	bool asynchronous;
	/// Интервал в микросекундах для проверки, не запрошена ли остановка выполнения запроса, и отправки прогресса.
	size_t interactive_delay;
	Poco::Timespan connect_timeout;
	Poco::Timespan connect_timeout_with_failover_ms;	/// Если следует выбрать одну из рабочих реплик.
	Poco::Timespan receive_timeout;
	Poco::Timespan send_timeout;
	/// Блокироваться в цикле ожидания запроса в сервере на указанное количество секунд.
	size_t poll_interval;
	/// Максимальное количество соединений с одним удалённым сервером в пуле.
	size_t distributed_connections_pool_size;
	/// Максимальное количество попыток соединения с репликами.
	size_t connections_with_failover_max_tries;

	/// Всевозможные ограничения на выполнение запроса.
	Limits limits;

	Settings() :
		max_block_size(DEFAULT_BLOCK_SIZE),
		max_threads(DEFAULT_MAX_THREADS),
		max_distributed_connections(DEFAULT_MAX_DISTRIBUTED_CONNECTIONS),
		max_query_size(DEFAULT_MAX_QUERY_SIZE),
		asynchronous(true),
		interactive_delay(DEFAULT_INTERACTIVE_DELAY),
		connect_timeout(DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, 0),
		connect_timeout_with_failover_ms(0, DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS),
		receive_timeout(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0),
		send_timeout(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0),
		poll_interval(DBMS_DEFAULT_POLL_INTERVAL),
		distributed_connections_pool_size(DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE),
		connections_with_failover_max_tries(DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES)
	{
	}

	/// Установить настройку по имени.
	void set(const String & name, const Field & value)
	{
			 if (name == "max_block_size")		max_block_size 		= safeGet<UInt64>(value);
		else if (name == "max_threads")			max_threads 		= safeGet<UInt64>(value);
		else if (name == "max_query_size")		max_query_size 		= safeGet<UInt64>(value);
		else if (name == "asynchronous")		asynchronous 		= safeGet<UInt64>(value);
		else if (name == "interactive_delay") 	interactive_delay 	= safeGet<UInt64>(value);
		else if (name == "connect_timeout")		connect_timeout 	= Poco::Timespan(safeGet<UInt64>(value), 0);
		else if (name == "receive_timeout")		receive_timeout 	= Poco::Timespan(safeGet<UInt64>(value), 0);
		else if (name == "send_timeout")		send_timeout 		= Poco::Timespan(safeGet<UInt64>(value), 0);
		else if (name == "poll_interval")		poll_interval 		= safeGet<UInt64>(value);
		else if (name == "connect_timeout_with_failover_ms")
			connect_timeout_with_failover_ms = Poco::Timespan(safeGet<UInt64>(value) * 1000);
		else if (name == "max_distributed_connections") max_distributed_connections = safeGet<UInt64>(value);
		else if (name == "distributed_connections_pool_size") distributed_connections_pool_size = safeGet<UInt64>(value);
		else if (name == "connections_with_failover_max_tries") connections_with_failover_max_tries = safeGet<UInt64>(value);
		else if (!limits.trySet(name, value))
			throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
	}

	/// Установить настройку по имени. Прочитать сериализованное значение из буфера.
	void set(const String & name, ReadBuffer & buf)
	{
		if (   name == "max_block_size"
			|| name == "max_threads"
			|| name == "max_query_size"
			|| name == "asynchronous"
			|| name == "interactive_delay"
			|| name == "connect_timeout"
			|| name == "receive_timeout"
			|| name == "send_timeout"
			|| name == "poll_interval"
			|| name == "connect_timeout_with_failover_ms"
			|| name == "max_distributed_connections"
			|| name == "distributed_connections_pool_size"
			|| name == "connections_with_failover_max_tries")
		{
			UInt64 value = 0;
			readVarUInt(value, buf);
			set(name, value);
		}
		else if (!limits.trySet(name, buf))
			throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
	}

	/// Прочитать настройки из буфера. Они записаны как набор name-value пар, идущих подряд, заканчивающихся пустым name.
	void deserialize(ReadBuffer & buf)
	{
		while (true)
		{
			String name;
			readBinary(name, buf);

			/// Пустая строка - это маркер конца настроек.
			if (name.empty())
				break;

			set(name, buf);
		}
	}

	/// Записать все настройки в буфер.
	void serialize(WriteBuffer & buf) const
	{
		writeStringBinary("max_block_size", buf);						writeVarUInt(max_block_size, buf);
		writeStringBinary("max_threads", buf);							writeVarUInt(max_threads, buf);
		writeStringBinary("max_query_size", buf);						writeVarUInt(max_query_size, buf);
		writeStringBinary("asynchronous", buf);							writeVarUInt(asynchronous, buf);
		writeStringBinary("interactive_delay", buf);					writeVarUInt(interactive_delay, buf);
		writeStringBinary("connect_timeout", buf);						writeVarUInt(connect_timeout.totalSeconds(), buf);
		writeStringBinary("receive_timeout", buf);						writeVarUInt(receive_timeout.totalSeconds(), buf);
		writeStringBinary("send_timeout", buf);							writeVarUInt(send_timeout.totalSeconds(), buf);
		writeStringBinary("poll_interval", buf);						writeVarUInt(poll_interval, buf);
		writeStringBinary("connect_timeout_with_failover_ms", buf);		writeVarUInt(connect_timeout_with_failover_ms.totalMilliseconds(), buf);
		writeStringBinary("max_distributed_connections", buf);			writeVarUInt(max_distributed_connections, buf);
		writeStringBinary("distributed_connections_pool_size", buf);	writeVarUInt(distributed_connections_pool_size, buf);
		writeStringBinary("connections_with_failover_max_tries", buf);	writeVarUInt(connections_with_failover_max_tries, buf);

		limits.serialize(buf);

		/// Пустая строка - это маркер конца настроек.
		writeStringBinary("", buf);
	}
};


}
