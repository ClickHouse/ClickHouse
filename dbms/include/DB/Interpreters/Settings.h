#pragma once

#include <Poco/Timespan.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Field.h>

#include <DB/Interpreters/Limits.h>
#include <DB/Interpreters/SettingsCommon.h>


namespace DB
{

/** Настройки выполнения запроса.
  */
struct Settings
{
	/** Перечисление настроек: тип, имя, значение по-умолчанию.
	  *
	  * Это сделано несколько неудобно, чтобы не перечислять настройки во многих разных местах.
	  * Замечание: можно было бы сделать полностью динамические настройки вида map: String -> Field,
	  *  но пока рано, так как в коде они используются как статический struct.
	  */

#define APPLY_FOR_SETTINGS(M) \
	/** При записи данных, для сжатия выделяется буфер размером max_compress_block_size. При переполнении буфера или если в буфер */ \
	/** записано данных больше или равно, чем min_compress_block_size, то при очередной засечке, данные так же будут сжиматься */ \
	/** В результате, для маленьких столбцов (числа 1-8 байт), при index_granularity = 8192, размер блока будет 64 KБ. */ \
	/** А для больших столбцов (Title - строка ~100 байт), размер блока будет ~819 КБ.  */ \
	/** За счёт этого, коэффициент сжатия почти не ухудшится.  */ \
	M(SettingUInt64, min_compress_block_size, DEFAULT_MIN_COMPRESS_BLOCK_SIZE) \
	M(SettingUInt64, max_compress_block_size, DEFAULT_MAX_COMPRESS_BLOCK_SIZE) \
	/** Максимальный размер блока для чтения */ \
	M(SettingUInt64, max_block_size, DEFAULT_BLOCK_SIZE) \
	/** Максимальный размер блока для вставки, если мы управляем формированием блоков для вставки. */ \
	M(SettingUInt64, max_insert_block_size, DEFAULT_INSERT_BLOCK_SIZE) \
	/** Максимальное количество потоков выполнения запроса. По-умолчанию - определять автоматически. */ \
	M(SettingMaxThreads, max_threads, 0) \
	/** Максимальный размер буфера для чтения из файловой системы. */ \
	M(SettingUInt64, max_read_buffer_size, DBMS_DEFAULT_BUFFER_SIZE) \
	/** Максимальное количество соединений при распределённой обработке одного запроса (должно быть больше, чем max_threads). */ \
	M(SettingUInt64, max_distributed_connections, DEFAULT_MAX_DISTRIBUTED_CONNECTIONS) \
	/** Какую часть запроса можно прочитать в оперативку для парсинга (оставшиеся данные для INSERT, если есть, считываются позже) */ \
	M(SettingUInt64, max_query_size, DEFAULT_MAX_QUERY_SIZE) \
	/** Интервал в микросекундах для проверки, не запрошена ли остановка выполнения запроса, и отправки прогресса. */ \
	M(SettingUInt64, interactive_delay, DEFAULT_INTERACTIVE_DELAY) \
	M(SettingSeconds, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC) \
	/** Если следует выбрать одну из рабочих реплик. */ \
	M(SettingMilliseconds, connect_timeout_with_failover_ms, DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS) \
	M(SettingSeconds, receive_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC) \
	M(SettingSeconds, send_timeout, DBMS_DEFAULT_SEND_TIMEOUT_SEC) \
	/** Время ожидания в очереди запросов, если количество одновременно выполняющихся запросов превышает максимальное. */ \
	M(SettingMilliseconds, queue_max_wait_ms, DEFAULT_QUERIES_QUEUE_WAIT_TIME_MS) \
	/** Блокироваться в цикле ожидания запроса в сервере на указанное количество секунд. */ \
	M(SettingUInt64, poll_interval, DBMS_DEFAULT_POLL_INTERVAL) \
	/** Максимальное количество соединений с одним удалённым сервером в пуле. */ \
	M(SettingUInt64, distributed_connections_pool_size, DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE) \
	/** Максимальное количество попыток соединения с репликами. */ \
	M(SettingUInt64, connections_with_failover_max_tries, DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES) \
	/** Считать минимумы и максимумы столбцов результата. Они могут выводиться в JSON-форматах. */ \
	M(SettingBool, extremes, false) \
	/** Использовать ли кэш разжатых блоков. */ \
	M(SettingBool, use_uncompressed_cache, true) \
	/** Следует ли отменять выполняющийся запрос с таким же id, как новый. */ \
	M(SettingBool, replace_running_query, false) \
	/** Количество потоков, выполняющих фоновую работу для таблиц (например, слияние в merge tree). \
	  * TODO: Сейчас применяется только при запуске сервера. Можно сделать изменяемым динамически. */ \
	M(SettingUInt64, background_pool_size, DBMS_DEFAULT_BACKGROUND_POOL_SIZE) \
	\
	/** Sleep time for StorageDistributed DirectoryMonitors in case there is no work or exception has been thrown */ \
	M(SettingMilliseconds, distributed_directory_monitor_sleep_time_ms, DBMS_DISTRIBUTED_DIRECTORY_MONITOR_SLEEP_TIME_MS) \
	\
	/** Allows disabling WHERE to PREWHERE optimization in SELECT queries from MergeTree */ \
	M(SettingBool, optimize_move_to_prewhere, false) \
	\
	/** Ожидать выполнения действий по манипуляции с партициями. 0 - не ждать, 1 - ждать выполнения только у себя, 2 - ждать всех. */ \
	M(SettingUInt64, replication_alter_partitions_sync, 1) \
	\
	M(SettingLoadBalancing, load_balancing, LoadBalancing::RANDOM) \
	\
	M(SettingTotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE) \
	M(SettingFloat, totals_auto_threshold, 0.5) \
	\
	/** Сэмплирование по умолчанию. Если равно 1, то отключено. */ \
	M(SettingFloat, default_sample, 1.0) \
	\
	/** Включена ли компиляция запросов. */ \
	M(SettingBool, compile, false) \
	/** Количество одинаковых по структуре запросов перед тем, как инициируется их компиляция. */ \
	M(SettingUInt64, min_count_to_compile, 3) \
	/** При каком количестве ключей, начинает использоваться двухуровневая агрегация. 0 - никогда не использовать. */ \
	M(SettingUInt64, group_by_two_level_threshold, 100000) \
	\
	/** Максимальное количество используемых реплик каждого шарда при выполнении запроса */ \
	M(SettingUInt64, max_parallel_replicas, 1) \
	M(SettingUInt64, parallel_replicas_count, 0) \
	M(SettingUInt64, parallel_replica_offset, 0) \
	\
	/** Тонкие настройки для чтения из MergeTree */ \
	\
	/** Если из одного файла читается хотя бы столько строк, чтение можно распараллелить. */ \
	M(SettingUInt64, merge_tree_min_rows_for_concurrent_read, (20 * 8192)) \
	/** Можно пропускать чтение более чем стольки строк ценой одного seek по файлу. */ \
	M(SettingUInt64, merge_tree_min_rows_for_seek, (5 * 8192)) \
	/** Если отрезок индекса может содержать нужные ключи, делим его на столько частей и рекурсивно проверяем их. */ \
	M(SettingUInt64, merge_tree_coarse_index_granularity, 8) \
	/** Максимальное количество строк на запрос, для использования кэша разжатых данных. Если запрос большой - кэш не используется. \
	  * (Чтобы большие запросы не вымывали кэш.) */ \
	M(SettingUInt64, merge_tree_max_rows_to_use_cache, (1024 * 1024)) \
	\
	/** Минимальная длина выражения expr = x1 OR ... expr = xN для оптимизации */ \
	M(SettingUInt64, optimize_min_equality_disjunction_chain_length, 3) \
	\
	/** Минимальное количество байт для операций ввода/ввывода минуя кэш страниц. 0 - отключено. */ \
	M(SettingUInt64, min_bytes_to_use_direct_io, 0) \
	\
	/** Кидать исключение, если есть индекс по дате, и он не используется. */ \
	M(SettingBool, force_index_by_date, 0) \
	\
	/** В запросе INSERT с указанием столбцов, заполнять значения по-умолчанию только для столбцов с явными DEFAULT-ами. */ \
	M(SettingBool, strict_insert_defaults, 0) \

	/// Всевозможные ограничения на выполнение запроса.
	Limits limits;

#define DECLARE(TYPE, NAME, DEFAULT) \
	TYPE NAME {DEFAULT};

	APPLY_FOR_SETTINGS(DECLARE)

#undef DECLARE

	/// Установить настройку по имени.
	void set(const String & name, const Field & value);

	/// Установить настройку по имени. Прочитать сериализованное в бинарном виде значение из буфера (для межсерверного взаимодействия).
	void set(const String & name, ReadBuffer & buf);

	/// Пропустить сериализованное в бинарном виде значение из буфера.
	void ignore(const String & name, ReadBuffer & buf);

	/** Установить настройку по имени. Прочитать значение в текстовом виде из строки (например, из конфига, или из параметра URL).
	  */
	void set(const String & name, const String & value);

	/** Установить настройки из профиля (в конфиге сервера, в одном профиле может быть перечислено много настроек).
	  * Профиль также может быть установлен с помощью функций set, как настройка profile.
	  */
	void setProfile(const String & profile_name, Poco::Util::AbstractConfiguration & config);

	/// Прочитать настройки из буфера. Они записаны как набор name-value пар, идущих подряд, заканчивающихся пустым name.
	/// Если в настройках выставлено readonly=1, то игнорировать настройки.
	void deserialize(ReadBuffer & buf);

	/// Записать изменённые настройки в буфер. (Например, для отправки на удалённый сервер.)
	void serialize(WriteBuffer & buf) const;
};


}
