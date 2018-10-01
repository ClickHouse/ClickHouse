
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_config .)

add_library(clickhouse_common_config ${LINK_MODE} ${clickhouse_common_config_headers} ${clickhouse_common_config_sources})

target_link_libraries (clickhouse_common_config clickhouse_common_zookeeper string_utils ${Poco_XML_LIBRARY} ${Poco_Util_LIBRARY})
target_include_directories (clickhouse_common_config PRIVATE ${DBMS_INCLUDE_DIR})
