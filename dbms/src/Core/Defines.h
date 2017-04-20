#pragma once

#define DBMS_NAME                                                "ClickHouse"
#define DBMS_VERSION_MAJOR                                         1
#define DBMS_VERSION_MINOR                                         1

#define DBMS_DEFAULT_HOST                                         "localhost"
#define DBMS_DEFAULT_PORT                                         9000
#define DBMS_DEFAULT_PORT_STR                                     #DBMS_DEFAULT_PORT
#define DBMS_DEFAULT_HTTP_PORT                                     8123
#define DBMS_DEFAULT_CONNECT_TIMEOUT_SEC                        10
#define DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS            50
#define DBMS_DEFAULT_SEND_TIMEOUT_SEC                            300
#define DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC                        300
/// Timeout for synchronous request-result protocol call (like Ping or TablesStatus).
#define DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC                   5
#define DBMS_DEFAULT_POLL_INTERVAL                                 10

/// Размер буфера ввода-вывода по-умолчанию.
#define DBMS_DEFAULT_BUFFER_SIZE                                 1048576ULL

/// При записи данных, для сжатия выделяется буфер размером max_compress_block_size. При переполнении буфера или если в буфер
/// записано данных больше или равно, чем min_compress_block_size, то при очередной засечке, данные так же будут сжиматься
/// В результате, для маленьких столбцов (числа 1-8 байт), при index_granularity = 8192, размер блока будет 64 KБ.
/// А для больших столбцов (Title - строка ~100 байт), размер блока будет ~819 КБ. За счёт этого, коэффициент сжатия почти не ухудшится.
#define DEFAULT_MIN_COMPRESS_BLOCK_SIZE                            65536
#define DEFAULT_MAX_COMPRESS_BLOCK_SIZE                         1048576

/** Какими блоками по-умолчанию читаются данные (в числе строк).
  * Меньшие значения дают лучшую кэш-локальность, меньшее потребление оперативки, но больший оверхед на обработку запроса.
  */
#define DEFAULT_BLOCK_SIZE                                         65536

/** Какие блоки следует формировать для вставки в таблицу, если мы управляем формированием блоков.
  * (Иногда в таблицу вставляются ровно такие блоки, какие были считаны / переданы извне, и на их размер этот параметр не влияет.)
  * Больше, чем DEFAULT_BLOCK_SIZE, так как в некоторых таблицах на каждый блок создаётся кусок данных на диске (довольно большая штука),
  *  и если бы куски были маленькими, то их было бы накладно потом объединять.
  */
#define DEFAULT_INSERT_BLOCK_SIZE                                1048576

/** То же самое, но для операций слияния. Меньше DEFAULT_BLOCK_SIZE для экономии оперативки (так как читаются все столбцы).
  * Сильно меньше, так как бывают 10-way слияния.
  */
#define DEFAULT_MERGE_BLOCK_SIZE                                 8192

#define DEFAULT_MAX_QUERY_SIZE                                     262144
#define SHOW_CHARS_ON_SYNTAX_ERROR                                 160L
#define DEFAULT_MAX_DISTRIBUTED_CONNECTIONS                        1024
#define DEFAULT_INTERACTIVE_DELAY                                100000
#define DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE             1024
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES     3
/// каждый период уменьшаем счетчик ошибок в 2 раза
/// слишком маленький период может приводить, что ошибки исчезают сразу после создания.
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD     (2 * DBMS_DEFAULT_SEND_TIMEOUT_SEC)
#define DEFAULT_QUERIES_QUEUE_WAIT_TIME_MS                         5000    /// Максимальное время ожидания в очереди запросов.
#define DBMS_DEFAULT_BACKGROUND_POOL_SIZE                        16

/// Используется в методе reserve, когда известно число строк, но неизвестны их размеры.
#define DBMS_APPROX_STRING_SIZE 64

/// Суффикс имени для столбца, содержащего смещения массива.
#define ARRAY_SIZES_COLUMN_NAME_SUFFIX                             ".size"

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES                    50264
#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS            51554
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO                        51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO                        54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE                    54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO            54060
#define DBMS_MIN_REVISION_WITH_TABLES_STATUS                      54226

/// Version of ClickHouse TCP protocol. Set to git tag with latest protocol change.
#define DBMS_TCP_PROTOCOL_VERSION                                54226

#define DBMS_DISTRIBUTED_DIRECTORY_MONITOR_SLEEP_TIME_MS        100

/// Граница, на которых должны быть выровнены блоки для асинхронных файловых операций.
#define DEFAULT_AIO_FILE_BLOCK_SIZE                                4096

#define DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS        7500

#define ALWAYS_INLINE     __attribute__((__always_inline__))
#define NO_INLINE         __attribute__((__noinline__))

#define PLATFORM_NOT_SUPPORTED    "The only supported platforms are x86_64 and AArch64 (work in progress)"

#if !defined(__x86_64__) && !defined(__aarch64__)
    #error PLATFORM_NOT_SUPPORTED
#endif

/// Check for presence of address sanitizer
#if defined(__has_feature)
    #if __has_feature(address_sanitizer)
        #define ADDRESS_SANITIZER 1
    #endif
#elif defined(__SANITIZE_ADDRESS__)
    #define ADDRESS_SANITIZER 1
#endif

#if defined(__has_feature)
    #if __has_feature(thread_sanitizer)
        #define THREAD_SANITIZER 1
    #endif
#elif defined(__SANITIZE_THREAD__)
    #define THREAD_SANITIZER 1
#endif
