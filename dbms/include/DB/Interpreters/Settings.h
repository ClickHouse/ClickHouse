#pragma once

#include <Poco/Timespan.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Field.h>

#include <DB/Interpreters/Limits.h>
#include <DB/Interpreters/SettingsCommon.h>


namespace DB
{

/** Settings of query execution.
  */
struct Settings
{
	/// For initialization from empty initializer-list to be "value initialization", not "aggregate initialization" in С++14.
	/// http://en.cppreference.com/w/cpp/language/aggregate_initialization
	Settings() {}

	/** List of settings: type, name, default value.
	  *
	  * This looks rather unconvenient. It is done that way to avoid repeating settings in different places.
	  * Note: as an alternative, we could implement settings to be completely dynamic in form of map: String -> Field,
	  *  but we are not going to do it, because settings is used everywhere as static struct fields.
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
	/** Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough. */ \
	M(SettingUInt64, min_insert_block_size_rows, DEFAULT_INSERT_BLOCK_SIZE) \
	/** Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough. */ \
	M(SettingUInt64, min_insert_block_size_bytes, (DEFAULT_INSERT_BLOCK_SIZE * 256)) \
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
	M(SettingBool, optimize_move_to_prewhere, true) \
	\
	/** Ожидать выполнения действий по манипуляции с партициями. 0 - не ждать, 1 - ждать выполнения только у себя, 2 - ждать всех. */ \
	M(SettingUInt64, replication_alter_partitions_sync, 1) \
	/** Ожидать выполнения действий по изменению структуры таблицы в течение указанного количества секунд. 0 - ждать неограниченное время. */ \
	M(SettingUInt64, replication_alter_columns_timeout, 60) \
	\
	M(SettingLoadBalancing, load_balancing, LoadBalancing::RANDOM) \
	\
	M(SettingTotalsMode, totals_mode, TotalsMode::AFTER_HAVING_EXCLUSIVE) \
	M(SettingFloat, totals_auto_threshold, 0.5) \
	\
	/** Включена ли компиляция запросов. */ \
	M(SettingBool, compile, false) \
	/** Количество одинаковых по структуре запросов перед тем, как инициируется их компиляция. */ \
	M(SettingUInt64, min_count_to_compile, 3) \
	/** При каком количестве ключей, начинает использоваться двухуровневая агрегация. 0 - порог не выставлен. */ \
	M(SettingUInt64, group_by_two_level_threshold, 100000) \
	/** При каком размере состояния агрегации в байтах, начинает использоваться двухуровневая агрегация. 0 - порог не выставлен. \
	  * Двухуровневая агрегация начинает использоваться при срабатывании хотя бы одного из порогов. */ \
	M(SettingUInt64, group_by_two_level_threshold_bytes, 100000000) \
	/** Включён ли экономный по памяти режим распределённой агрегации. */ \
	M(SettingBool, distributed_aggregation_memory_efficient, false) \
	/** Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. \
	  * 0 means - same as 'max_threads'. */ \
	M(SettingUInt64, aggregation_memory_efficient_merge_threads, 0) \
	\
	/** Максимальное количество используемых реплик каждого шарда при выполнении запроса */ \
	M(SettingUInt64, max_parallel_replicas, 1) \
	M(SettingUInt64, parallel_replicas_count, 0) \
	M(SettingUInt64, parallel_replica_offset, 0) \
	\
	/** Тихо пропускать недоступные шарды. */ \
	M(SettingBool, skip_unavailable_shards, false) \
	\
	/** Не мерджить состояния агрегации с разных серверов при распределённой обработке запроса \
	  *  - на случай, когда доподлинно известно, что на разных шардах разные ключи. \
	  */ \
	M(SettingBool, distributed_group_by_no_merge, false) \
	\
	/** Тонкие настройки для чтения из MergeTree */ \
	\
	/** Если из одного файла читается хотя бы столько строк, чтение можно распараллелить. */ \
	M(SettingUInt64, merge_tree_min_rows_for_concurrent_read, (20 * 8192)) \
	/** Можно пропускать чтение более чем стольки строк ценой одного seek по файлу. */ \
	M(SettingUInt64, merge_tree_min_rows_for_seek, 0) \
	/** Если отрезок индекса может содержать нужные ключи, делим его на столько частей и рекурсивно проверяем их. */ \
	M(SettingUInt64, merge_tree_coarse_index_granularity, 8) \
	/** Максимальное количество строк на запрос, для использования кэша разжатых данных. Если запрос большой - кэш не используется. \
	  * (Чтобы большие запросы не вымывали кэш.) */ \
	M(SettingUInt64, merge_tree_max_rows_to_use_cache, (1024 * 1024)) \
	\
	/** Распределять чтение из MergeTree по потокам равномерно, обеспечивая стабильное среднее время исполнения каждого потока в пределах одного чтения. */ \
	M(SettingBool, merge_tree_uniform_read_distribution, true) \
	\
	/** Минимальная длина выражения expr = x1 OR ... expr = xN для оптимизации */ \
	M(SettingUInt64, optimize_min_equality_disjunction_chain_length, 3) \
	\
	/** Минимальное количество байт для операций ввода/ввывода минуя кэш страниц. 0 - отключено. */ \
	M(SettingUInt64, min_bytes_to_use_direct_io, 0) \
	\
	/** Кидать исключение, если есть индекс, и он не используется. */ \
	M(SettingBool, force_index_by_date, 0) \
	M(SettingBool, force_primary_key, 0) \
	\
	/** В запросе INSERT с указанием столбцов, заполнять значения по-умолчанию только для столбцов с явными DEFAULT-ами. */ \
	M(SettingBool, strict_insert_defaults, 0) \
	\
	/** В случае превышения максимального размера mark_cache, удалять только записи, старше чем mark_cache_min_lifetime секунд. */ \
	M(SettingUInt64, mark_cache_min_lifetime, 10000) \
	\
	/** Позволяет использовать больше источников, чем количество потоков - для более равномерного распределения работы по потокам. \
	  * Предполагается, что это временное решение, так как можно будет в будущем сделать количество источников равное количеству потоков, \
	  *  но чтобы каждый источник динамически выбирал себе доступную работу. \
	  */ \
	M(SettingFloat, max_streams_to_max_threads_ratio, 1) \
	\
	/** Позволяет выбирать метод сжатия данных при записи */\
	M(SettingCompressionMethod, network_compression_method, CompressionMethod::LZ4) \
	\
	/** Приоритет запроса. 1 - самый высокий, больше - ниже; 0 - не использовать приоритеты. */ \
	M(SettingUInt64, priority, 0) \
	\
	/** Логгировать запросы и писать лог в системную таблицу. */ \
	M(SettingBool, log_queries, 0) \
	\
	/** If query length is greater than specified threshold (in bytes), then cut query when writing to query log. \
	  * Also limit length of printed query in ordinary text log. \
	  */ \
	M(SettingUInt64, log_queries_cut_to_length, 100000) \
	\
	/** Как выполняются распределённые подзапросы внутри секций IN или JOIN? */ \
	M(SettingDistributedProductMode, distributed_product_mode, DistributedProductMode::DENY) \
	\
	/** Схема выполнения GLOBAL-подзапросов. */ \
	M(SettingGlobalSubqueriesMethod, global_subqueries_method, GlobalSubqueriesMethod::PUSH) \
	\
	/** Максимальное количество одновременно выполняющихся запросов на одного user-а. */ \
	M(SettingUInt64, max_concurrent_queries_for_user, 0) \
	\
	/** Для запросов INSERT в реплицируемую таблицу, ждать записи на указанное число реплик и лианеризовать добавление данных. 0 - отключено. */ \
	M(SettingUInt64, insert_quorum, 0) \
	M(SettingMilliseconds, insert_quorum_timeout, 600000) \
	/** Для запросов SELECT из реплицируемой таблицы, кидать исключение, если на реплике нет куска, записанного с кворумом; \
	  * не читать куски, которые ещё не были записаны с кворумом. */ \
	M(SettingUInt64, select_sequential_consistency, 0) \
	/** Максимальное количество различных шардов и максимальное количество реплик одного шарда в функции remote. */ \
	M(SettingUInt64, table_function_remote_max_addresses, 1000) \
	/** Маскимальное количество потоков при распределённой обработке одного запроса */ \
	M(SettingUInt64, max_distributed_processing_threads, 8) \
	\
	/** Настройки понижения числа потоков в случае медленных чтений. */ \
	/** Обращать внимания только на чтения, занявшие не меньше такого количества времени. */ \
	M(SettingMilliseconds, 	read_backoff_min_latency_ms, 1000) \
	/** Считать события, когда пропускная способность меньше стольки байт в секунду. */ \
	M(SettingUInt64, 		read_backoff_max_throughput, 1048576) \
	/** Не обращать внимания на событие, если от предыдущего прошло меньше стольки-то времени. */ \
	M(SettingMilliseconds, 	read_backoff_min_interval_between_events_ms, 1000) \
	/** Количество событий, после которого количество потоков будет уменьшено. */ \
	M(SettingUInt64, 		read_backoff_min_events, 2) \
	\
	/** В целях тестирования exception safety - кидать исключение при каждом выделении памяти с указанной вероятностью. */ \
	M(SettingFloat, memory_tracker_fault_probability, 0.) \
	\
	/** Сжимать результат, если клиент по HTTP сказал, что он понимает данные, сжатые методом gzip или deflate */ \
	M(SettingBool, enable_http_compression, 0) \
	/** Уровень сжатия - используется, если клиент по HTTP сказал, что он понимает данные, сжатые методом gzip или deflate */ \
	M(SettingInt64, http_zlib_compression_level, 3) \
	\
	/** При разжатии данных POST от клиента, сжатых родным форматом, не проверять чексуммы */ \
	M(SettingBool, http_native_compression_disable_checksumming_on_decompress, 0) \
	\
	/** Таймаут в секундах */ \
	M(SettingUInt64, resharding_barrier_timeout, 300) \
	\
	/** What aggregate function to use for implementation of count(DISTINCT ...) */ \
	M(SettingString, count_distinct_implementation, "uniqExact") \
	\
	/** Write statistics about read rows, bytes, time elapsed in suitable output formats */ \
	M(SettingBool, output_format_write_statistics, true) \
	\
	/** Write add http CORS header */ \
	M(SettingBool, add_http_cors_header, false) \
	\
	/** Skip columns with unknown names from input data (it works for JSONEachRow and TSKV formats). */ \
	M(SettingBool, input_format_skip_unknown_fields, false) \
	\
	/** For Values format: if field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression. */ \
	M(SettingBool, input_format_values_interpret_expressions, true) \
	\
	/** Controls quoting of 64-bit integers in JSON output format. */ \
	M(SettingBool, output_format_json_quote_64bit_integers, true) \
	\
	/** Rows limit for Pretty formats. */ \
	M(SettingUInt64, output_format_pretty_max_rows, 10000) \
	\
	/** Use client timezone for interpreting DateTime string values, instead of adopting server timezone. */ \
	M(SettingBool, use_client_time_zone, false) \
	\
	/** Send progress notifications using X-ClickHouse-Progress headers. \
	  * Some clients do not support high amount of HTTP headers (Python requests in particular), so it is disabled by default. \
	  */ \
	M(SettingBool, send_progress_in_http_headers, false) \
	\
	/** Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval. */ \
	M(SettingUInt64, http_headers_progress_interval_ms, 100) \
	\
	/** Do fsync after changing metadata for tables and databases (.sql files). \
	  * Could be disabled in case of poor latency on server with high load of DDL queries and high load of disk subsystem. \
	  */ \
	M(SettingBool, fsync_metadata, 1) \
	\
	/** Maximum amount of errors while reading text formats (like CSV, TSV). \
	  * In case of error, if both values are non-zero, \
	  *  and at least absolute or relative amount of errors is lower than corresponding value, \
	  *  will skip until next line and continue. \
	  */ \
	M(SettingUInt64, input_format_allow_errors_num, 0) \
	M(SettingFloat, input_format_allow_errors_ratio, 0) \
	\
	/** Size of buffer with query results */ \
	M(SettingUInt64, send_buffer_size, DBMS_DEFAULT_BUFFER_SIZE) \
	M(SettingUInt64, http_response_memory_buffer_size, 0) \
	M(SettingString, http_response_memory_buffer_overflow, "") \

	/// Possible limits for query execution.
	Limits limits;

#define DECLARE(TYPE, NAME, DEFAULT) \
	TYPE NAME {DEFAULT};

	APPLY_FOR_SETTINGS(DECLARE)

#undef DECLARE

	/// Set setting by name.
	void set(const String & name, const Field & value);

	/// Set setting by name. Read value, serialized in binary form from buffer (for inter-server communication).
	void set(const String & name, ReadBuffer & buf);

	/// Skip value, serialized in binary form in buffer.
	void ignore(const String & name, ReadBuffer & buf);

	/// Set setting by name. Read value in text form from string (for example, from configuration file or from URL parameter).
	void set(const String & name, const String & value);

	/** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
	  * Профиль также может быть установлен с помощью функций set, как настройка profile.
	  */
	void setProfile(const String & profile_name, Poco::Util::AbstractConfiguration & config);

	/// Load settings from configuration file, at "path" prefix in configuration.
	void loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

	/// Read settings from buffer. They are serialized as list of contiguous name-value pairs, finished with empty name.
	/// If readonly=1 is set, ignore read settings.
	void deserialize(ReadBuffer & buf);

	/// Write changed settings to buffer. (For example, to be sent to remote server.)
	void serialize(WriteBuffer & buf) const;
};


}
