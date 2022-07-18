include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_dictionaries .)

add_headers_and_sources(clickhouse_dictionaries "${CMAKE_CURRENT_BINARY_DIR}/generated/")

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")

    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    set_source_files_properties(
        FlatDictionary.cpp
        HashedDictionary.cpp
        HashedArrayDictionary.cpp
        CacheDictionary.cpp
        RangeHashedDictionary.cpp
        DirectDictionary.cpp
        PROPERTIES COMPILE_FLAGS -g0)
endif ()

list(REMOVE_ITEM clickhouse_dictionaries_sources DictionaryFactory.cpp DictionarySourceFactory.cpp DictionaryStructure.cpp getDictionaryConfigurationFromAST.cpp)
list(REMOVE_ITEM clickhouse_dictionaries_headers DictionaryFactory.h DictionarySourceFactory.h DictionaryStructure.h getDictionaryConfigurationFromAST.h)

add_library(clickhouse_dictionaries ${clickhouse_dictionaries_sources})

target_link_libraries(clickhouse_dictionaries
    PRIVATE
        clickhouse_common_io
        dbms
        Poco::Data
        Poco::Data::ODBC
        Poco::MongoDB
        Poco::Redis
        string_utils
)

target_link_libraries(clickhouse_dictionaries PUBLIC ch_contrib::abseil_swiss_tables)

if (TARGET ch_contrib::cassandra)
    target_link_libraries(clickhouse_dictionaries PRIVATE ch_contrib::cassandra)
endif()

add_subdirectory(Embedded)
target_link_libraries(clickhouse_dictionaries PRIVATE ch_contrib::sparsehash)
