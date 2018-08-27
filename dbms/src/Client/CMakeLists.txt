# TODO: make separate lib datastream, block, ...
#include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
#add_headers_and_sources(clickhouse_client .)
#add_library(clickhouse_client ${LINK_MODE} ${clickhouse_client_headers} ${clickhouse_client_sources})
#target_link_libraries (clickhouse_client clickhouse_common_io ${Poco_Net_LIBRARY})
#target_include_directories (clickhouse_client PRIVATE ${DBMS_INCLUDE_DIR})
