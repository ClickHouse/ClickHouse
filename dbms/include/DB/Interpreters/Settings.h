#pragma once

#include <Poco/Timespan.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Field.h>

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
	/// Переписывать запросы SELECT из CollapsingMergeTree с агрегатными функциями
	/// для автоматического учета поля Sign
	bool sign_rewrite;

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
		connections_with_failover_max_tries(DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES),
		sign_rewrite(false)
	{
	}

	/// Установить настройку по имени.
	void set(const String & name, const Field & value);

	/// Установить настройку по имени. Прочитать сериализованное в бинарном виде значение из буфера (для межсерверного взаимодействия).
	void set(const String & name, ReadBuffer & buf);

	/// Установить настройку по имени. Прочитать значение в текстовом виде из строки (например, из конфига, или из параметра URL).
	void set(const String & name, const String & value);

	/** Установить настройки из профиля (в конфиге сервера, в одном профиле может быть перечислено много настроек).
	  * Профиль также может быть установлен с помощью функций set, как настройка profile.
	  */
	void setProfile(const String & profile_name);

	/// Прочитать настройки из буфера. Они записаны как набор name-value пар, идущих подряд, заканчивающихся пустым name.
	void deserialize(ReadBuffer & buf);

	/// Записать все настройки в буфер.
	void serialize(WriteBuffer & buf) const;
};


}
