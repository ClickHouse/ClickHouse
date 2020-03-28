include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_dictionaries .)

add_headers_and_sources(clickhouse_dictionaries ${CMAKE_CURRENT_BINARY_DIR}/generated/)

list(REMOVE_ITEM clickhouse_dictionaries_sources DictionaryFactory.cpp DictionarySourceFactory.cpp DictionaryStructure.cpp getDictionaryConfigurationFromAST.cpp)
list(REMOVE_ITEM clickhouse_dictionaries_headers DictionaryFactory.h DictionarySourceFactory.h DictionaryStructure.h getDictionaryConfigurationFromAST.h)

add_library(clickhouse_dictionaries ${clickhouse_dictionaries_sources})
target_link_libraries(clickhouse_dictionaries PRIVATE dbms clickhouse_common_io string_utils ${BTRIE_LIBRARIES})

if(Poco_SQL_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
endif()

if(USE_POCO_SQLODBC)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_SQLODBC_LIBRARY} ${Poco_SQL_LIBRARY})
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_SQLODBC_INCLUDE_DIR} ${Poco_SQL_INCLUDE_DIR})
endif()

if(Poco_Data_FOUND)
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
endif()

if(USE_POCO_DATAODBC)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_DataODBC_LIBRARY} ${Poco_Data_LIBRARY})
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_DataODBC_INCLUDE_DIR})
endif()

if(USE_POCO_MONGODB)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_MongoDB_LIBRARY})
endif()

if(USE_POCO_REDIS)
    # for code highlighting in CLion
    # target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_Redis_INCLUDE_DIR})

    # for build
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_Redis_LIBRARY})
endif()

add_subdirectory(Embedded)

target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${SPARSEHASH_INCLUDE_DIR})
