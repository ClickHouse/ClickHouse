include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_uatraitsfast .)
add_headers_and_sources(clickhouse_common_uatraitsfast types)

add_library(clickhouse_common_uatraitsfast ${clickhouse_common_uatraitsfast_sources})
target_link_libraries(clickhouse_common_uatraitsfast PUBLIC common PRIVATE clickhouse_common_zookeeper string_utils PUBLIC ${Poco_XML_LIBRARY} ${Poco_Util_LIBRARY})
target_include_directories(clickhouse_common_uatraitsfast PUBLIC ${DBMS_INCLUDE_DIR})

