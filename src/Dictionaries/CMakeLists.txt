include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_dictionaries .)

add_headers_and_sources(clickhouse_dictionaries ${CMAKE_CURRENT_BINARY_DIR}/generated/)

list(REMOVE_ITEM clickhouse_dictionaries_sources DictionaryFactory.cpp DictionarySourceFactory.cpp DictionaryStructure.cpp getDictionaryConfigurationFromAST.cpp)
list(REMOVE_ITEM clickhouse_dictionaries_headers DictionaryFactory.h DictionarySourceFactory.h DictionaryStructure.h getDictionaryConfigurationFromAST.h)

add_library(clickhouse_dictionaries ${clickhouse_dictionaries_sources})

target_link_libraries(clickhouse_dictionaries
    PRIVATE
        ${BTRIE_LIBRARIES}
        clickhouse_common_io
        dbms
        Poco::Data
        Poco::Data::ODBC
        Poco::MongoDB
        Poco::Redis
        string_utils
)

if(USE_CASSANDRA)
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${CASSANDRA_INCLUDE_DIR})
endif()

add_subdirectory(Embedded)

target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${SPARSEHASH_INCLUDE_DIR})
