set(CLICKHOUSE_LOCAL_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/LocalServer.cpp)
set(CLICKHOUSE_LOCAL_LINK PRIVATE clickhouse_dictionaries clickhouse_common_io clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions ${Boost_PROGRAM_OPTIONS_LIBRARY})
#set(CLICKHOUSE_LOCAL_INCLUDE SYSTEM PRIVATE ...)

clickhouse_program_add(local)

if(NOT CLICKHOUSE_ONE_SHARED)
    target_link_libraries(clickhouse-local-lib PRIVATE clickhouse-server-lib)
endif ()
