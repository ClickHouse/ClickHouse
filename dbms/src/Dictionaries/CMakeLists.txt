include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_generate_function.cmake)

add_headers_and_sources(clickhouse_dictionaries .)
add_headers_and_sources(clickhouse_dictionaries ./Embedded)

list(REMOVE_ITEM clickhouse_dictionaries_sources DictionarySourceFactory.cpp)
list(REMOVE_ITEM clickhouse_dictionaries_headers DictionarySourceFactory.h)


add_library(clickhouse_dictionaries ${clickhouse_dictionaries_sources})
target_link_libraries(clickhouse_dictionaries dbms)
#target_include_directories (clickhouse_dictionaries BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libfarmhash)
#target_include_directories (clickhouse_dictionaries BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)
#target_include_directories (clickhouse_dictionaries BEFORE PUBLIC ${DIVIDE_INCLUDE_DIR})
