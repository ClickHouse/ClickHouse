add_library (daemon
    BaseDaemon.cpp
    GraphiteWriter.cpp
    SentryWriter.cpp
)

target_link_libraries (daemon PUBLIC loggers common PRIVATE clickhouse_parsers clickhouse_common_io clickhouse_common_config)

if (TARGET ch_contrib::sentry)
    target_link_libraries (daemon PRIVATE ch_contrib::sentry dbms)
endif ()
