if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
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

include(cmake/find_vectorclass.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config.h)

include (cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL} revision ${VERSION_REVISION} ${VERSION_OFFICIAL}")
configure_file (src/Common/config.h.in ${CONFIG_COMMON})
configure_file (src/Common/config_version.h.in ${CONFIG_VERSION})

if (NOT MSVC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif ()

if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

# Add some warnings that are not available even with -Wall -Wextra -Wpedantic.

option (WEVERYTHING "Enables -Weverything option with some exceptions. This is intended for exploration of new compiler warnings that may be found to be useful. Only makes sense for clang." ON)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wpedantic -Wno-vla-extension -Wno-zero-length-array -Wno-gnu-anonymous-struct -Wno-nested-anon-types")

    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wshadow -Wshadow-uncaptured-local -Wextra-semi -Wcomma -Winconsistent-missing-destructor-override -Wunused-exception-parameter -Wcovered-switch-default -Wold-style-cast -Wrange-loop-analysis -Wunused-member-function -Wunreachable-code -Wunreachable-code-return -Wnewline-eof -Wembedded-directive -Wgnu-case-range -Wunused-macros -Wconditional-uninitialized -Wdeprecated -Wundef -Wreserved-id-macro -Wredundant-parens -Wzero-as-null-pointer-constant")

    if (WEVERYTHING)
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-missing-noreturn -Wno-padded -Wno-switch-enum -Wno-shadow-field-in-constructor -Wno-deprecated-dynamic-exception-spec -Wno-float-equal -Wno-weak-vtables -Wno-shift-sign-overflow -Wno-sign-conversion -Wno-conversion -Wno-exit-time-destructors -Wno-undefined-func-template -Wno-documentation-unknown-command -Wno-missing-variable-declarations -Wno-unused-template -Wno-global-constructors -Wno-c99-extensions -Wno-missing-prototypes -Wno-weak-template-vtables -Wno-zero-length-array -Wno-gnu-anonymous-struct -Wno-nested-anon-types -Wno-double-promotion -Wno-disabled-macro-expansion -Wno-used-but-marked-unused -Wno-vla-extension -Wno-vla -Wno-packed")

        # TODO Enable conversion, sign-conversion, double-promotion warnings.
    endif ()

    if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7)
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

        if (WEVERYTHING)
            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-return-std-move-in-c++11")
        endif ()
    endif ()

    if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8)
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra-semi-stmt -Wshadow-field -Wstring-plus-int -Wempty-init-stmt")
    endif ()

    if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
        if (WEVERYTHING)
            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-ctad-maybe-unsupported")
        endif ()
    endif ()
endif ()

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-include ${ClickHouse_SOURCE_DIR}/libs/libcommon/include/common/iostream_debug_helpers.h")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # If we leave this optimization enabled, gcc-7 replaces a pair of SSE intrinsics (16 byte load, store) with a call to memcpy.
    # It leads to slow code. This is compiler bug. It looks like this:
    #
    # (gdb) bt
    #0  memcpy (destination=0x7faa6e9f1638, source=0x7faa81d9e9a8, size=16) at ../libs/libmemcpy/memcpy.h:11
    #1  0x0000000005341c5f in _mm_storeu_si128 (__B=..., __P=<optimized out>) at /usr/lib/gcc/x86_64-linux-gnu/7/include/emmintrin.h:720
    #2  memcpySmallAllowReadWriteOverflow15Impl (n=<optimized out>, src=<optimized out>, dst=<optimized out>) at ../dbms/src/Common/memcpySmall.h:37

    add_definitions ("-fno-tree-loop-distribute-patterns")
endif ()

add_subdirectory (src)

set(dbms_headers)
set(dbms_sources)

include(../cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_io src/Common)
add_headers_and_sources(clickhouse_common_io src/Common/HashTable)
add_headers_and_sources(clickhouse_common_io src/IO)

add_headers_and_sources(dbms src/Core)
add_headers_and_sources(dbms src/DataStreams)
add_headers_and_sources(dbms src/DataTypes)
add_headers_and_sources(dbms src/Databases)
add_headers_and_sources(dbms src/Interpreters)
add_headers_and_sources(dbms src/Interpreters/ClusterProxy)
add_headers_and_sources(dbms src/Columns)
add_headers_and_sources(dbms src/Storages)
add_headers_and_sources(dbms src/Storages/Distributed)
add_headers_and_sources(dbms src/Storages/MergeTree)
add_headers_and_sources(dbms src/Client)
add_headers_and_sources(dbms src/Formats)

list (APPEND clickhouse_common_io_sources ${CONFIG_BUILD})
list (APPEND clickhouse_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

list (APPEND dbms_sources src/Functions/IFunction.cpp src/Functions/FunctionFactory.cpp src/Functions/FunctionHelpers.cpp)
list (APPEND dbms_headers src/Functions/IFunction.h src/Functions/FunctionFactory.h src/Functions/FunctionHelpers.h)

list (APPEND dbms_sources
    src/AggregateFunctions/AggregateFunctionFactory.cpp
    src/AggregateFunctions/AggregateFunctionCombinatorFactory.cpp
    src/AggregateFunctions/AggregateFunctionState.cpp
    src/AggregateFunctions/parseAggregateFunctionParameters.cpp)

list (APPEND dbms_headers
    src/AggregateFunctions/IAggregateFunction.h
    src/AggregateFunctions/IAggregateFunctionCombinator.h
    src/AggregateFunctions/AggregateFunctionFactory.h
    src/AggregateFunctions/AggregateFunctionCombinatorFactory.h
    src/AggregateFunctions/AggregateFunctionState.h
    src/AggregateFunctions/FactoryHelpers.h
    src/AggregateFunctions/parseAggregateFunctionParameters.h)

list (APPEND dbms_sources src/TableFunctions/ITableFunction.cpp src/TableFunctions/TableFunctionFactory.cpp)
list (APPEND dbms_headers src/TableFunctions/ITableFunction.h   src/TableFunctions/TableFunctionFactory.h)
list (APPEND dbms_sources src/Dictionaries/DictionaryFactory.cpp src/Dictionaries/DictionarySourceFactory.cpp src/Dictionaries/DictionaryStructure.cpp)
list (APPEND dbms_headers src/Dictionaries/DictionaryFactory.h   src/Dictionaries/DictionarySourceFactory.h   src/Dictionaries/DictionaryStructure.h)

add_library(clickhouse_common_io ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

if (OS_FREEBSD)
    target_compile_definitions (clickhouse_common_io PUBLIC CLOCK_MONOTONIC_COARSE=CLOCK_MONOTONIC_FAST)
endif ()

add_subdirectory(src/Common/ZooKeeper)
add_subdirectory(src/Common/Config)

if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    add_library(dbms ${dbms_headers} ${dbms_sources})
else ()
    add_library(dbms SHARED ${dbms_headers} ${dbms_sources})
endif ()

if (USE_EMBEDDED_COMPILER)
    llvm_libs_all(REQUIRED_LLVM_LIBRARIES)
    target_link_libraries (dbms PRIVATE ${REQUIRED_LLVM_LIBRARIES})
    target_include_directories (dbms SYSTEM BEFORE PUBLIC ${LLVM_INCLUDE_DIRS})
endif ()

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    set_source_files_properties(
        src/Dictionaries/FlatDictionary.cpp
        src/Dictionaries/HashedDictionary.cpp
        src/Dictionaries/CacheDictionary.cpp
        src/Dictionaries/TrieDictionary.cpp
        src/Dictionaries/RangeHashedDictionary.cpp
        src/Dictionaries/ComplexKeyHashedDictionary.cpp
        src/Dictionaries/ComplexKeyCacheDictionary.cpp
        src/Dictionaries/ComplexKeyCacheDictionary_generate1.cpp
        src/Dictionaries/ComplexKeyCacheDictionary_generate2.cpp
        src/Dictionaries/ComplexKeyCacheDictionary_generate3.cpp
        src/Dictionaries/ODBCBlockInputStream.cpp
        src/Dictionaries/HTTPDictionarySource.cpp
        src/Dictionaries/LibraryDictionarySource.cpp
        src/Dictionaries/ExecutableDictionarySource.cpp
        src/Dictionaries/ClickHouseDictionarySource.cpp
        PROPERTIES COMPILE_FLAGS -g0)
endif ()

target_link_libraries (clickhouse_common_io
        PUBLIC
    common
        PRIVATE
    string_utils
    widechar_width
    ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
    ${DOUBLE_CONVERSION_LIBRARIES}
        PUBLIC
    ${Poco_Net_LIBRARY}
    ${Poco_Util_LIBRARY}
    ${Poco_Foundation_LIBRARY}
    ${RE2_LIBRARY}
    ${RE2_ST_LIBRARY}
    ${CITYHASH_LIBRARIES}
        PRIVATE
    ${ZLIB_LIBRARIES}
    ${EXECINFO_LIBRARIES}
        PUBLIC
    ${Boost_SYSTEM_LIBRARY}
        PRIVATE
    apple_rt
        PUBLIC
    Threads::Threads
        PRIVATE
    ${CMAKE_DL_LIBS}
        PUBLIC
    roaring
)


target_include_directories(clickhouse_common_io SYSTEM BEFORE PUBLIC ${RE2_INCLUDE_DIR})

if (USE_LFALLOC)
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${LFALLOC_INCLUDE_DIR})
endif ()

if(CPUID_LIBRARY)
    target_link_libraries(clickhouse_common_io PRIVATE ${CPUID_LIBRARY})
endif()

if(CPUINFO_LIBRARY)
    target_link_libraries(clickhouse_common_io PRIVATE ${CPUINFO_LIBRARY})
endif()

target_link_libraries (dbms
        PUBLIC
    clickhouse_compression
        PRIVATE
    clickhouse_parsers
    clickhouse_common_config
        PUBLIC
    clickhouse_common_io
        PRIVATE
    clickhouse_dictionaries_embedded
        PUBLIC
    ${MYSQLXX_LIBRARY}
        PRIVATE
    ${BTRIE_LIBRARIES}
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
        PUBLIC
    ${Boost_SYSTEM_LIBRARY}
    Threads::Threads
)

target_include_directories(dbms SYSTEM BEFORE PUBLIC ${PDQSORT_INCLUDE_DIR})

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
endif ()

if (Poco_SQL_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
    target_include_directories (dbms SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
endif()

if (USE_POCO_SQLODBC)
    target_link_libraries (clickhouse_common_io PRIVATE ${Poco_SQL_LIBRARY})
    target_link_libraries (dbms PRIVATE ${Poco_SQLODBC_LIBRARY} ${Poco_SQL_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_SQL_INCLUDE_DIR})
        target_include_directories (dbms SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_SQLODBC_INCLUDE_DIR} SYSTEM PUBLIC ${Poco_SQL_INCLUDE_DIR})
    endif()
endif()

if (Poco_Data_FOUND)
    target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
    target_include_directories (dbms SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
endif()

if (USE_POCO_DATAODBC)
    target_link_libraries (clickhouse_common_io PRIVATE ${Poco_Data_LIBRARY})
    target_link_libraries (dbms PRIVATE ${Poco_DataODBC_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        target_include_directories (dbms SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_DataODBC_INCLUDE_DIR})
    endif()
endif()

if (USE_POCO_MONGODB)
    target_link_libraries (dbms PRIVATE ${Poco_MongoDB_LIBRARY})
endif()

if (USE_POCO_NETSSL)
    target_link_libraries (clickhouse_common_io PRIVATE ${Poco_NetSSL_LIBRARY} ${Poco_Crypto_LIBRARY})
    target_link_libraries (dbms PRIVATE ${Poco_NetSSL_LIBRARY} ${Poco_Crypto_LIBRARY})
endif()

target_link_libraries (dbms PRIVATE ${Poco_Foundation_LIBRARY})

if (USE_ICU)
    target_link_libraries (dbms PRIVATE ${ICU_LIBRARIES})
    target_include_directories (dbms SYSTEM PRIVATE ${ICU_INCLUDE_DIRS})
endif ()

if (USE_CAPNP)
    target_link_libraries (dbms PRIVATE ${CAPNP_LIBRARY})
    if (NOT USE_INTERNAL_CAPNP_LIBRARY)
        target_include_directories (dbms SYSTEM BEFORE PRIVATE ${CAPNP_INCLUDE_DIR})
    endif ()
endif ()

if (USE_RDKAFKA)
    target_link_libraries (dbms PRIVATE clickhouse_storage_kafka)
endif ()

if (USE_PARQUET)
    target_link_libraries(dbms PRIVATE ${PARQUET_LIBRARY})
    if (NOT USE_INTERNAL_PARQUET_LIBRARY OR USE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE)
        target_include_directories (dbms SYSTEM BEFORE PRIVATE ${PARQUET_INCLUDE_DIR} ${ARROW_INCLUDE_DIR})
    endif ()
endif ()

if(OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries(dbms PRIVATE ${OPENSSL_CRYPTO_LIBRARY})
endif ()
target_link_libraries(dbms PRIVATE Threads::Threads)

target_include_directories (dbms SYSTEM BEFORE PRIVATE ${DIVIDE_INCLUDE_DIR})
target_include_directories (dbms SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})

if (USE_PROTOBUF)
    target_link_libraries (dbms PRIVATE ${Protobuf_LIBRARY})
    target_include_directories (dbms SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR})
endif ()

if (USE_HDFS)
    target_link_libraries (clickhouse_common_io PRIVATE ${HDFS3_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PRIVATE ${HDFS3_INCLUDE_DIR})
endif()

if (USE_BROTLI)
    target_link_libraries (clickhouse_common_io PRIVATE ${BROTLI_LIBRARY})
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PRIVATE ${BROTLI_INCLUDE_DIR})
endif()

if (USE_JEMALLOC)
    target_include_directories (dbms SYSTEM BEFORE PRIVATE ${JEMALLOC_INCLUDE_DIR}) # used in Interpreters/AsynchronousMetrics.cpp
endif ()

target_include_directories (dbms PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io SYSTEM PUBLIC ${PCG_RANDOM_INCLUDE_DIR})
target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

# also for copy_headers.sh:
target_include_directories (clickhouse_common_io BEFORE PRIVATE ${COMMON_INCLUDE_DIR})

add_subdirectory (programs)
add_subdirectory (tests)

if (ENABLE_TESTS AND USE_GTEST)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources(${ClickHouse_SOURCE_DIR}/dbms dbms_gtest_sources)
    add_executable(unit_tests_dbms ${dbms_gtest_sources})
    target_link_libraries(unit_tests_dbms PRIVATE ${GTEST_BOTH_LIBRARIES} dbms clickhouse_common_zookeeper)
    add_check(unit_tests_dbms)
endif ()
