add_library (daemon
    BaseDaemon.cpp
    GraphiteWriter.cpp
)

target_include_directories (daemon PUBLIC ..)
target_link_libraries (daemon PUBLIC loggers PRIVATE clickhouse_common_io clickhouse_common_config common ${EXECINFO_LIBRARIES})
