include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_uatraitsfast .)
add_headers_and_sources(clickhouse_common_uatraitsfast types)

add_library(clickhouse_common_uatraitsfast ${clickhouse_common_uatraitsfast_headers} ${clickhouse_common_uatraitsfast_sources})
target_include_directories (clickhouse_common_uatraitsfast PRIVATE clickhouse_common_io PUBLIC ${Poco_XML_LIBRARY} ${Poco_Util_LIBRARY} )

