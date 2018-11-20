include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_generate_function.cmake)

add_headers_and_sources(clickhouse_dictionaries .)

add_library(clickhouse_dictionaries ${LINK_MODE} ${clickhouse_dictionaries_sources})
target_link_libraries(clickhouse_dictionaries PRIVATE clickhouse_common_io ${MYSQLXX_LIBRARY} ${BTRIE_LIBRARIES})

if(Poco_SQL_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
endif()

if(USE_POCO_SQLODBC)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_SQLODBC_LIBRARY} ${Poco_SQL_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_SQLODBC_INCLUDE_DIR} ${Poco_SQL_INCLUDE_DIR})
    endif()
endif()

if(Poco_Data_FOUND)
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
endif()

if(USE_POCO_DATAODBC)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_DataODBC_LIBRARY} ${Poco_Data_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_DataODBC_INCLUDE_DIR})
    endif()
endif()

if(USE_POCO_MONGODB)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_MongoDB_LIBRARY})
endif()

add_subdirectory(Embedded)
