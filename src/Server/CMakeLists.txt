set (SRCS
    HTTPHandler.cpp
    HTTPHandlerFactory.cpp
    InterserverIOHTTPHandler.cpp
    MySQLHandler.cpp
    MySQLHandlerFactory.cpp
    NotFoundHandler.cpp
    PrometheusMetricsWriter.cpp
    PrometheusRequestHandler.cpp
    ReplicasStatusHandler.cpp
    StaticRequestHandler.cpp
    TCPHandler.cpp
)

add_library (clickhouse_server ${SRCS})
target_link_libraries (clickhouse_server
    PRIVATE
        clickhouse_access
        clickhouse_client
        clickhouse_compression
        clickhouse_core
        clickhouse_datastreams
        clickhouse_disks
        clickhouse_interpreters
        clickhouse_parsers
        clickhouse_processors_executors
        clickhouse_storages
        Poco::Util
        string_utils)
