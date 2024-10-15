include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_dictionaries .)

add_headers_and_sources(clickhouse_dictionaries "${CMAKE_CURRENT_BINARY_DIR}/generated/")

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    set_source_files_properties(
        FlatDictionary.cpp
        HashedDictionary.cpp
        SparseHashedDictionary.cpp
        HashedArrayDictionary.cpp
        CacheDictionary.cpp
        RangeHashedDictionary.cpp
        DirectDictionary.cpp
        PROPERTIES COMPILE_FLAGS -g0)
endif()

extract_into_parent_list(clickhouse_dictionaries_sources dbms_sources
    DictionaryFactory.cpp # DictionaryFactory::instance() Used by ExternalDictionariesLoader.cpp, getDictionaryConfigurationFromAST.cpp...
    DictionarySourceFactory.cpp # DictionarySourceFactory::instance() Used by DictionaryFactory.cpp
    DictionaryStructure.cpp # DictionaryStructure::DictionaryStructure (ExternalDictionariesLoader.cpp)
    getDictionaryConfigurationFromAST.cpp # getDictionaryConfigurationFromAST (StorageDictionary.cpp, AST Visitors)
)
extract_into_parent_list(clickhouse_dictionaries_headers dbms_headers
    DictionaryFactory.h
    DictionarySourceFactory.h
    DictionaryStructure.h
    getDictionaryConfigurationFromAST.h
)

add_library(clickhouse_dictionaries ${clickhouse_dictionaries_headers} ${clickhouse_dictionaries_sources})

target_link_libraries(clickhouse_dictionaries
    PRIVATE
        clickhouse_common_io
        dbms
        Poco::Data
        Poco::Redis
)

if (USE_MONGODB)
    target_link_libraries(clickhouse_dictionaries PRIVATE Poco::MongoDB)
endif()

target_link_libraries(clickhouse_dictionaries PUBLIC ch_contrib::abseil_swiss_tables)

if (TARGET ch_contrib::cassandra)
    target_link_libraries(clickhouse_dictionaries PRIVATE ch_contrib::cassandra)
endif()

if (TARGET ch_contrib::yaml_cpp)
    target_link_libraries(clickhouse_dictionaries PRIVATE ch_contrib::yaml_cpp)
endif()

if (TARGET ch_contrib::vectorscan)
    target_link_libraries(clickhouse_dictionaries PRIVATE ch_contrib::vectorscan)
endif()

add_subdirectory(Embedded)
target_link_libraries(clickhouse_dictionaries PRIVATE ch_contrib::sparsehash)
