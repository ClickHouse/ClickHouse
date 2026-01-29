add_library(clickhouse_processors_substrait
    SubstraitSerializer.cpp
)

target_link_libraries(clickhouse_processors_substrait
    PRIVATE
        ch_contrib::substrait
        dbms
)

target_include_directories(clickhouse_processors_substrait SYSTEM PRIVATE
    ${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src
)
