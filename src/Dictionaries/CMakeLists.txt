include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_dictionaries .)

add_headers_and_sources(clickhouse_dictionaries "${CMAKE_CURRENT_BINARY_DIR}/generated/")

# Libcxx hardening means out-of-bounds checks which causes noticeable degradations for some workflows.
# TUs where libcxx hardening needs to be disabled are specified here.
set(LIBCPP_HARDENING_DISABLE_FILES
    HashedDictionary.cpp
    HashedArrayDictionary.cpp
    RangeHashedDictionary.cpp
)
set_property(SOURCE ${LIBCPP_HARDENING_DISABLE_FILES}
    PROPERTY COMPILE_FLAGS "-U_LIBCPP_HARDENING_MODE -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_NONE")

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    set_property(SOURCE
        ${LIBCPP_HARDENING_DISABLE_FILES}
        FlatDictionary.cpp
        SparseHashedDictionary.cpp
        CacheDictionary.cpp
        DirectDictionary.cpp
        APPEND_STRING PROPERTY COMPILE_FLAGS " -g1")
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
        Poco::Redis
)

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
