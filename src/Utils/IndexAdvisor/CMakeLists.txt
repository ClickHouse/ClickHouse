add_headers_and_sources(dbms Utils/IndexAdvisor)

add_library(clickhouse_index_advisor STATIC
    ${dbms_sources}
    ${dbms_headers}
)

target_link_libraries(clickhouse_index_advisor
    PRIVATE
        clickhouse_parsers
        clickhouse_common_io
        dbms
) 