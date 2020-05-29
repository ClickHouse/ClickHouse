if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if(COMPILER_PIPE)
    set(MAX_COMPILER_MEMORY 2500)
else()
    set(MAX_COMPILER_MEMORY 1500)
endif()
if(MAKE_STATIC_LIBRARIES)
    set(MAX_LINKER_MEMORY 3500)
else()
    set(MAX_LINKER_MEMORY 2500)
endif()
include(../cmake/limit_jobs.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/Common/config.h)

include (../cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL} revision ${VERSION_REVISION} ${VERSION_OFFICIAL}")
configure_file (Common/config.h.in ${CONFIG_COMMON})
configure_file (Common/config_version.h.in ${CONFIG_VERSION})
configure_file (Core/config_core.h.in ${CMAKE_CURRENT_BINARY_DIR}/Core/include/config_core.h)

if (COMPILER_GCC)
    # If we leave this optimization enabled, gcc-7 replaces a pair of SSE intrinsics (16 byte load, store) with a call to memcpy.
    # It leads to slow code. This is compiler bug. It looks like this:
    #
    # (gdb) bt
    #0  memcpy (destination=0x7faa6e9f1638, source=0x7faa81d9e9a8, size=16) at ../libs/libmemcpy/memcpy.h:11
    #1  0x0000000005341c5f in _mm_storeu_si128 (__B=..., __P=<optimized out>) at /usr/lib/gcc/x86_64-linux-gnu/7/include/emmintrin.h:720
    #2  memcpySmallAllowReadWriteOverflow15Impl (n=<optimized out>, src=<optimized out>, dst=<optimized out>) at ../src/Common/memcpySmall.h:37

    add_definitions ("-fno-tree-loop-distribute-patterns")
endif ()

add_subdirectory (Access)
add_subdirectory (Columns)
add_subdirectory (Common)
add_subdirectory (Core)
add_subdirectory (DataStreams)
add_subdirectory (DataTypes)
add_subdirectory (Dictionaries)
add_subdirectory (Disks)
add_subdirectory (Storages)
add_subdirectory (Parsers)
add_subdirectory (IO)
add_subdirectory (Functions)
add_subdirectory (Interpreters)
add_subdirectory (AggregateFunctions)
add_subdirectory (Client)
add_subdirectory (TableFunctions)
add_subdirectory (Processors)
add_subdirectory (Formats)
add_subdirectory (Compression)
add_subdirectory (Server)


set(dbms_headers)
set(dbms_sources)

add_headers_and_sources(clickhouse_common_io Common)
add_headers_and_sources(clickhouse_common_io Common/HashTable)
add_headers_and_sources(clickhouse_common_io IO)
list (REMOVE_ITEM clickhouse_common_io_sources Common/malloc.cpp Common/new_delete.cpp)

if(USE_RDKAFKA)
    add_headers_and_sources(dbms Storages/Kafka)
endif()

if (USE_AWS_S3)
    add_headers_and_sources(dbms Disks/S3)
endif()

list (APPEND clickhouse_common_io_sources ${CONFIG_BUILD})
list (APPEND clickhouse_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

list (APPEND dbms_sources Functions/IFunction.cpp Functions/FunctionFactory.cpp Functions/FunctionHelpers.cpp Functions/extractTimeZoneFromFunctionArguments.cpp)
list (APPEND dbms_headers Functions/IFunctionImpl.h Functions/FunctionFactory.h Functions/FunctionHelpers.h Functions/extractTimeZoneFromFunctionArguments.h)

list (APPEND dbms_sources
    AggregateFunctions/AggregateFunctionFactory.cpp
    AggregateFunctions/AggregateFunctionCombinatorFactory.cpp
    AggregateFunctions/AggregateFunctionState.cpp
    AggregateFunctions/parseAggregateFunctionParameters.cpp)

list (APPEND dbms_headers
    AggregateFunctions/IAggregateFunction.h
    AggregateFunctions/IAggregateFunctionCombinator.h
    AggregateFunctions/AggregateFunctionFactory.h
    AggregateFunctions/AggregateFunctionCombinatorFactory.h
    AggregateFunctions/AggregateFunctionState.h
    AggregateFunctions/FactoryHelpers.h
    AggregateFunctions/parseAggregateFunctionParameters.h)

list (APPEND dbms_sources TableFunctions/ITableFunction.cpp TableFunctions/TableFunctionFactory.cpp)
list (APPEND dbms_headers TableFunctions/ITableFunction.h   TableFunctions/TableFunctionFactory.h)
list (APPEND dbms_sources Dictionaries/DictionaryFactory.cpp Dictionaries/DictionarySourceFactory.cpp Dictionaries/DictionaryStructure.cpp  Dictionaries/getDictionaryConfigurationFromAST.cpp)
list (APPEND dbms_headers Dictionaries/DictionaryFactory.h   Dictionaries/DictionarySourceFactory.h   Dictionaries/DictionaryStructure.h Dictionaries/getDictionaryConfigurationFromAST.h)

if (NOT ENABLE_SSL)
    list (REMOVE_ITEM clickhouse_common_io_sources Common/OpenSSLHelpers.cpp)
    list (REMOVE_ITEM clickhouse_common_io_headers Common/OpenSSLHelpers.h)
endif ()

add_library(clickhouse_common_io ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

add_library (clickhouse_malloc OBJECT Common/malloc.cpp)
set_source_files_properties(Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

add_library (clickhouse_new_delete STATIC Common/new_delete.cpp)
target_link_libraries (clickhouse_new_delete PRIVATE clickhouse_common_io jemalloc)

add_subdirectory(Common/ZooKeeper)
add_subdirectory(Common/Config)

set (all_modules)
macro(add_object_library name common_path)
    if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
        add_headers_and_sources(dbms ${common_path})
    else ()
        list (APPEND all_modules ${name})
        add_headers_and_sources(${name} ${common_path})
        add_library(${name} SHARED ${${name}_sources} ${${name}_headers})
        target_link_libraries (${name} PRIVATE -Wl,--unresolved-symbols=ignore-all)
    endif ()
endmacro()

add_object_library(clickhouse_access Access)
add_object_library(clickhouse_core Core)
add_object_library(clickhouse_compression Compression)
add_object_library(clickhouse_datastreams DataStreams)
add_object_library(clickhouse_datatypes DataTypes)
add_object_library(clickhouse_databases Databases)
add_object_library(clickhouse_disks Disks)
add_object_library(clickhouse_interpreters Interpreters)
add_object_library(clickhouse_interpreters_clusterproxy Interpreters/ClusterProxy)
add_object_library(clickhouse_columns Columns)
add_object_library(clickhouse_storages Storages)
add_object_library(clickhouse_storages_distributed Storages/Distributed)
add_object_library(clickhouse_storages_mergetree Storages/MergeTree)
add_object_library(clickhouse_storages_liveview Storages/LiveView)
add_object_library(clickhouse_client Client)
add_object_library(clickhouse_server Server)
add_object_library(clickhouse_formats Formats)
add_object_library(clickhouse_processors Processors)
add_object_library(clickhouse_processors_executors Processors/Executors)
add_object_library(clickhouse_processors_formats Processors/Formats)
add_object_library(clickhouse_processors_formats_impl Processors/Formats/Impl)
add_object_library(clickhouse_processors_transforms Processors/Transforms)
add_object_library(clickhouse_processors_sources Processors/Sources)
add_object_library(clickhouse_processors_merges Processors/Merges)
add_object_library(clickhouse_processors_merges_algorithms Processors/Merges/Algorithms)


if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    add_library (dbms STATIC ${dbms_headers} ${dbms_sources})
    target_link_libraries (dbms PRIVATE jemalloc)
    set (all_modules dbms)
else()
    add_library (dbms SHARED ${dbms_headers} ${dbms_sources})
    target_link_libraries (dbms PUBLIC ${all_modules})
    target_link_libraries (clickhouse_interpreters PRIVATE jemalloc)
    list (APPEND all_modules dbms)
    # force all split libs to be linked
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-as-needed")
endif ()

macro (dbms_target_include_directories)
    foreach (module ${all_modules})
        target_include_directories (${module} ${ARGN})
    endforeach ()
endmacro ()

macro (dbms_target_link_libraries)
    foreach (module ${all_modules})
        target_link_libraries (${module} ${ARGN})
    endforeach ()
endmacro ()

if (USE_EMBEDDED_COMPILER)
    dbms_target_link_libraries (PRIVATE ${REQUIRED_LLVM_LIBRARIES})
    dbms_target_include_directories (SYSTEM BEFORE PUBLIC ${LLVM_INCLUDE_DIRS})
endif ()

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    set_source_files_properties(
        Dictionaries/FlatDictionary.cpp
        Dictionaries/HashedDictionary.cpp
        Dictionaries/CacheDictionary.cpp
        Dictionaries/TrieDictionary.cpp
        Dictionaries/RangeHashedDictionary.cpp
        Dictionaries/ComplexKeyHashedDictionary.cpp
        Dictionaries/ComplexKeyCacheDictionary.cpp
        Dictionaries/ComplexKeyCacheDictionary_generate1.cpp
        Dictionaries/ComplexKeyCacheDictionary_generate2.cpp
        Dictionaries/ComplexKeyCacheDictionary_generate3.cpp
        Dictionaries/ODBCBlockInputStream.cpp
        Dictionaries/HTTPDictionarySource.cpp
        Dictionaries/LibraryDictionarySource.cpp
        Dictionaries/ExecutableDictionarySource.cpp
        Dictionaries/ClickHouseDictionarySource.cpp
        PROPERTIES COMPILE_FLAGS -g0)
endif ()

# Otherwise it will slow down stack traces printing too much.
set_source_files_properties(
        Common/Elf.cpp
        Common/Dwarf.cpp
        Common/SymbolIndex.cpp
        PROPERTIES COMPILE_FLAGS "-O3 ${WITHOUT_COVERAGE}")

target_link_libraries (clickhouse_common_io
        PRIVATE
            string_utils
            widechar_width
            ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
            common
            ${DOUBLE_CONVERSION_LIBRARIES}
            ryu
)

if(RE2_LIBRARY)
    target_link_libraries(clickhouse_common_io PUBLIC ${RE2_LIBRARY})
endif()
if(RE2_ST_LIBRARY)
    target_link_libraries(clickhouse_common_io PUBLIC ${RE2_ST_LIBRARY})
endif()

target_link_libraries(clickhouse_common_io
        PRIVATE
            ${EXECINFO_LIBRARIES}
            cpuid
        PUBLIC
            ${Boost_PROGRAM_OPTIONS_LIBRARY}
            ${Boost_SYSTEM_LIBRARY}
            ${CITYHASH_LIBRARIES}
            ${ZLIB_LIBRARIES}
            pcg_random
            Poco::Foundation
            roaring
)

if (USE_RDKAFKA)
    dbms_target_link_libraries(PRIVATE ${CPPKAFKA_LIBRARY} ${RDKAFKA_LIBRARY})
    if(NOT USE_INTERNAL_RDKAFKA_LIBRARY)
        dbms_target_include_directories(SYSTEM BEFORE PRIVATE ${RDKAFKA_INCLUDE_DIR})
    endif()
endif()


if(RE2_INCLUDE_DIR)
    target_include_directories(clickhouse_common_io SYSTEM BEFORE PUBLIC ${RE2_INCLUDE_DIR})
endif()

dbms_target_link_libraries (
    PRIVATE
        ${BTRIE_LIBRARIES}
        ${Boost_PROGRAM_OPTIONS_LIBRARY}
        ${Boost_FILESYSTEM_LIBRARY}
        ${LZ4_LIBRARY}
        clickhouse_parsers
        clickhouse_common_config
        clickhouse_common_zookeeper
        clickhouse_dictionaries_embedded
        Poco::JSON
        string_utils
    PUBLIC
        ${Boost_SYSTEM_LIBRARY}
        ${MYSQLXX_LIBRARY}
        clickhouse_common_io
)

target_include_directories(clickhouse_common_io PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/Core/include) # uses some includes from core
dbms_target_include_directories(PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/Core/include)

dbms_target_include_directories(SYSTEM BEFORE PUBLIC ${PDQSORT_INCLUDE_DIR})

if (NOT USE_INTERNAL_LZ4_LIBRARY AND LZ4_INCLUDE_DIR)
    dbms_target_include_directories(SYSTEM BEFORE PRIVATE ${LZ4_INCLUDE_DIR})
endif ()

if (ZSTD_LIBRARY)
    dbms_target_link_libraries(PRIVATE ${ZSTD_LIBRARY})
    if (NOT USE_INTERNAL_ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
        dbms_target_include_directories(SYSTEM BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
    endif ()
endif()

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
endif ()

if (USE_ICU)
    dbms_target_link_libraries (PRIVATE ${ICU_LIBRARIES})
    dbms_target_include_directories (SYSTEM PRIVATE ${ICU_INCLUDE_DIRS})
endif ()

if (USE_CAPNP)
    dbms_target_link_libraries (PRIVATE ${CAPNP_LIBRARIES})
endif ()

if (USE_PARQUET)
    dbms_target_link_libraries(PRIVATE ${PARQUET_LIBRARY})
    if (NOT USE_INTERNAL_PARQUET_LIBRARY OR USE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE)
        dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})
    endif ()
endif ()

if (USE_AVRO)
    dbms_target_link_libraries(PRIVATE ${AVROCPP_LIBRARY})
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${AVROCPP_INCLUDE_DIR})
endif ()

if (OPENSSL_CRYPTO_LIBRARY)
    dbms_target_link_libraries (PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
    target_link_libraries (clickhouse_common_io PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif ()

if (USE_LDAP)
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${OPENLDAP_INCLUDE_DIR})
    dbms_target_link_libraries (PRIVATE ${OPENLDAP_LIBRARIES})
endif ()

dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${DIVIDE_INCLUDE_DIR})
dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})

if (USE_PROTOBUF)
    dbms_target_link_libraries (PRIVATE ${Protobuf_LIBRARY})
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR})
endif ()

if (USE_HDFS)
    target_link_libraries (clickhouse_common_io PUBLIC ${HDFS3_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${HDFS3_INCLUDE_DIR})
endif()

if (USE_AWS_S3)
    target_link_libraries (clickhouse_common_io PUBLIC ${AWS_S3_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${AWS_S3_CORE_INCLUDE_DIR})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${AWS_S3_INCLUDE_DIR})
endif()

if (USE_BROTLI)
    target_link_libraries (clickhouse_common_io PRIVATE ${BROTLI_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PRIVATE ${BROTLI_INCLUDE_DIR})
endif()

if (USE_OPENCL)
    target_link_libraries (clickhouse_common_io PRIVATE ${OpenCL_LIBRARIES})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PRIVATE ${OpenCL_INCLUDE_DIRS})
endif ()

dbms_target_include_directories (PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io PUBLIC ${DBMS_INCLUDE_DIR})

target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${MSGPACK_INCLUDE_DIR})

if (ENABLE_TESTS AND USE_GTEST)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources(${ClickHouse_SOURCE_DIR}/src dbms_gtest_sources)
    add_executable(unit_tests_dbms ${dbms_gtest_sources})

    # gtest framework has substandard code
    target_compile_options(unit_tests_dbms PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-undef
        -Wno-sign-compare
        -Wno-used-but-marked-unused
        -Wno-missing-noreturn
        -Wno-gnu-zero-variadic-macro-arguments
    )

    target_link_libraries(unit_tests_dbms PRIVATE ${GTEST_BOTH_LIBRARIES} clickhouse_functions clickhouse_parsers dbms clickhouse_common_zookeeper string_utils)
    add_check(unit_tests_dbms)
endif ()
