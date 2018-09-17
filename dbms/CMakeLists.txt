if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/find_vectorclass.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config.h)
set (CONFIG_BUILD ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_build.cpp)

include (cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL}")
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config.h.in ${CONFIG_COMMON})
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config_version.h.in ${CONFIG_VERSION})

get_property (BUILD_COMPILE_DEFINITIONS DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY COMPILE_DEFINITIONS)
get_property (BUILD_INCLUDE_DIRECTORIES DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
string (TIMESTAMP BUILD_DATE "%Y-%m-%d" UTC)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config_build.cpp.in ${CONFIG_BUILD})

if (NOT MSVC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif ()

if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

# Add some warnings that are not available even with -Wall -Wextra.

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra-semi -Wcomma -Winconsistent-missing-destructor-override -Wunused-exception-parameter -Wshadow-uncaptured-local")

    if (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6)
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wredundant-parens -Wzero-as-null-pointer-constant")
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

find_package (Threads)

add_subdirectory (src)

set(dbms_headers)
set(dbms_sources)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_io src/Common)
add_headers_and_sources(clickhouse_common_io src/Common/HashTable)
add_headers_and_sources(clickhouse_common_io src/IO)

add_headers_and_sources(dbms src/Analyzers)
add_headers_and_sources(dbms src/Core)
add_headers_and_sources(dbms src/DataStreams)
add_headers_and_sources(dbms src/DataTypes)
add_headers_and_sources(dbms src/Databases)
add_headers_and_sources(dbms src/Dictionaries)
add_headers_and_sources(dbms src/Dictionaries/Embedded)
add_headers_and_sources(dbms src/Dictionaries/Embedded/GeodataProviders)
add_headers_and_sources(dbms src/Interpreters)
add_headers_and_sources(dbms src/Interpreters/ClusterProxy)
add_headers_and_sources(dbms src/Columns)
add_headers_and_sources(dbms src/Storages)
add_headers_and_sources(dbms src/Storages/Distributed)
add_headers_and_sources(dbms src/Storages/Kafka)
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
    src/AggregateFunctions/FactoryHelpers.cpp
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
list (APPEND dbms_headers src/TableFunctions/ITableFunction.h src/TableFunctions/TableFunctionFactory.h)

add_library(clickhouse_common_io ${LINK_MODE} ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

if (OS_FREEBSD)
    target_compile_definitions (clickhouse_common_io PUBLIC CLOCK_MONOTONIC_COARSE=CLOCK_MONOTONIC_FAST)
endif ()

add_subdirectory(src/Common/ZooKeeper)
add_subdirectory(src/Common/Config)

if (MAKE_STATIC_LIBRARIES)
    add_library(dbms ${dbms_headers} ${dbms_sources})
else ()
    add_library(dbms SHARED ${dbms_headers} ${dbms_sources})
    set_target_properties (dbms PROPERTIES SOVERSION ${VERSION_MAJOR}.${VERSION_MINOR} VERSION ${VERSION_SO} OUTPUT_NAME clickhouse)
    install (TARGETS dbms LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT clickhouse)
endif ()

if (USE_EMBEDDED_COMPILER)
    llvm_libs_all(REQUIRED_LLVM_LIBRARIES)
    target_link_libraries (dbms ${REQUIRED_LLVM_LIBRARIES})
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

if (NOT ARCH_ARM)
    set (LINK_LIBRARIES_ONLY_ON_X86_64 ${CPUID_LIBRARY})
endif()

target_link_libraries (clickhouse_common_io
    common
    string_utils
    ${LINK_LIBRARIES_ONLY_ON_X86_64}
    ${LZ4_LIBRARY}
    ${ZSTD_LIBRARY}
    ${DOUBLE_CONVERSION_LIBRARIES}
    ${Poco_Net_LIBRARY}
    ${Poco_Util_LIBRARY}
    ${Poco_Foundation_LIBRARY}
    ${ZLIB_LIBRARIES}
    ${EXECINFO_LIBRARY}
    ${ELF_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    apple_rt
    ${CMAKE_DL_LIBS}
)

target_link_libraries (dbms
    clickhouse_parsers
    clickhouse_common_config
    clickhouse_common_io
    ${MYSQLXX_LIBRARY}
    ${RE2_LIBRARY}
    ${RE2_ST_LIBRARY}
    ${BTRIE_LIBRARIES}
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
)

if (NOT USE_INTERNAL_RE2_LIBRARY)
    target_include_directories (dbms SYSTEM BEFORE PRIVATE ${RE2_INCLUDE_DIR})
endif ()

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
endif ()

if (Poco_SQL_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
    target_include_directories (dbms SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
endif()

if (USE_POCO_SQLODBC)
    target_link_libraries (clickhouse_common_io ${Poco_SQL_LIBRARY})
    target_link_libraries (dbms ${Poco_SQLODBC_LIBRARY} ${Poco_SQL_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_SQL_INCLUDE_DIR})
        target_include_directories (dbms SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_SQLODBC_INCLUDE_DIR} PUBLIC ${Poco_SQL_INCLUDE_DIR})
    endif()
endif()

#if (Poco_Data_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
if (Poco_Data_FOUND)
    target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
    target_include_directories (dbms SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
endif()

if (USE_POCO_DATAODBC)
    target_link_libraries (clickhouse_common_io ${Poco_Data_LIBRARY})
    target_link_libraries (dbms ${Poco_DataODBC_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        target_include_directories (dbms SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_DataODBC_INCLUDE_DIR})
    endif()
endif()

if (USE_POCO_MONGODB)
    target_link_libraries (dbms ${Poco_MongoDB_LIBRARY})
endif()

if (USE_POCO_NETSSL)
    target_link_libraries (clickhouse_common_io ${Poco_NetSSL_LIBRARY} ${Poco_Crypto_LIBRARY})
    target_link_libraries (dbms ${Poco_NetSSL_LIBRARY} ${Poco_Crypto_LIBRARY})
endif()

target_link_libraries (dbms ${Poco_Foundation_LIBRARY})

if (USE_ICU)
    target_link_libraries (dbms ${ICU_LIBS})
    target_include_directories (dbms SYSTEM PRIVATE ${ICU_INCLUDE_DIR})
endif ()

if (USE_CAPNP)
    target_link_libraries (dbms ${CAPNP_LIBRARY})
    if (NOT USE_INTERNAL_CAPNP_LIBRARY)
        target_include_directories (dbms SYSTEM BEFORE PRIVATE ${CAPNP_INCLUDE_DIR})
    endif ()
endif ()

if (USE_RDKAFKA)
    target_link_libraries (dbms ${RDKAFKA_LIBRARY})
    if (NOT USE_INTERNAL_RDKAFKA_LIBRARY)
        target_include_directories (dbms SYSTEM BEFORE PRIVATE ${RDKAFKA_INCLUDE_DIR})
    endif ()
endif ()

target_link_libraries(dbms ${OPENSSL_CRYPTO_LIBRARY})

target_link_libraries (dbms
    Threads::Threads
)

target_include_directories (dbms SYSTEM BEFORE PRIVATE ${DIVIDE_INCLUDE_DIR})
target_include_directories (dbms SYSTEM BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    target_include_directories (dbms SYSTEM BEFORE PRIVATE ${LZ4_INCLUDE_DIR})
endif ()
if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    target_include_directories (dbms SYSTEM BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()

target_include_directories (dbms PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io SYSTEM PUBLIC ${PCG_RANDOM_INCLUDE_DIR})
target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

# also for copy_headers.sh:
target_include_directories (clickhouse_common_io BEFORE PRIVATE ${COMMON_INCLUDE_DIR})

add_subdirectory (programs)
add_subdirectory (tests)

if (ENABLE_TESTS)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources(${ClickHouse_SOURCE_DIR}/dbms dbms_gtest_sources)
    add_executable(unit_tests_dbms ${dbms_gtest_sources})
    target_link_libraries(unit_tests_dbms gtest_main dbms)
    add_check(unit_tests_dbms)
endif ()
