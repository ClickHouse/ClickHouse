add_headers_and_sources(loggers .)

# Standard version depends on DBMS and works with text log
add_library(loggers ${loggers_sources} ${loggers_headers})
target_link_libraries(loggers PRIVATE dbms clickhouse_common_io)
