#pragma once

#define DBMS_NAME												"ClickHouse"
#define DBMS_VERSION_MAJOR 										0
#define DBMS_VERSION_MINOR 										0

#define DBMS_DEFAULT_HOST 										"localhost"
#define DBMS_DEFAULT_PORT 										9000
#define DBMS_DEFAULT_PORT_STR 									"9000"
#define DBMS_DEFAULT_CONNECT_TIMEOUT_SEC						10
#define DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS			50
#define DBMS_DEFAULT_SEND_TIMEOUT_SEC							300
#define DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC						300
#define DBMS_DEFAULT_POLL_INTERVAL 								10

#define DEFAULT_BLOCK_SIZE 										1048576
#define DEFAULT_MAX_QUERY_SIZE 									1048576
#define SHOW_CHARS_ON_SYNTAX_ERROR 								160L
#define DEFAULT_MAX_THREADS 									8
#define DEFAULT_MAX_DISTRIBUTED_CONNECTIONS						100
#define DEFAULT_INTERACTIVE_DELAY								100000
#define DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE 			128
#define DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES 	3

#define DBMS_MIN_REVISION_WITH_PER_QUERY_SETTINGS				28558

/// Используется в методе reserve, когда известно число строк, но неизвестны их размеры.
#define DBMS_APPROX_STRING_SIZE 64

/// Суффикс имени для столбца, содержащего смещения массива.
#define ARRAY_SIZES_COLUMN_NAME_SUFFIX 							".size"
