add_library (daemon
    BaseDaemon.cpp
    GraphiteWriter.cpp
    CrashWriter.cpp
)

target_link_libraries (daemon PUBLIC loggers common PRIVATE clickhouse_parsers clickhouse_common_io clickhouse_common_config)
