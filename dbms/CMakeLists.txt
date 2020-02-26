set(ConfigIncludePath ${CMAKE_CURRENT_BINARY_DIR}/includes/configs CACHE INTERNAL "Path to generated configuration files.")
include_directories(${ConfigIncludePath})

if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
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
configure_file (src/Core/config_core.h.in ${CMAKE_CURRENT_BINARY_DIR}/src/Core/include/config_core.h)

if (NOT MSVC)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
endif ()

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-I${ClickHouse_SOURCE_DIR}/base -include ${ClickHouse_SOURCE_DIR}/dbms/src/Core/iostream_debug_helpers.h")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

# Add some warnings that are not available even with -Wall -Wextra -Wpedantic.

option (WEVERYTHING "Enables -Weverything option with some exceptions. This is intended for exploration of new compiler warnings that may be found to be useful. Only makes sense for clang." ON)

if (COMPILER_CLANG)
    add_warning(pedantic)
    no_warning(gnu-anonymous-struct)
    no_warning(nested-anon-types)
    no_warning(vla-extension)
    no_warning(zero-length-array)

    add_warning(comma)
    add_warning(conditional-uninitialized)
    add_warning(covered-switch-default)
    add_warning(deprecated)
    add_warning(embedded-directive)
    add_warning(empty-init-stmt) # linux-only
    add_warning(extra-semi-stmt) # linux-only
    add_warning(extra-semi)
    add_warning(gnu-case-range)
    add_warning(inconsistent-missing-destructor-override)
    add_warning(newline-eof)
    add_warning(old-style-cast)
    add_warning(range-loop-analysis)
    add_warning(redundant-parens)
    add_warning(reserved-id-macro)
    add_warning(shadow-field) # clang 8+
    add_warning(shadow-uncaptured-local)
    add_warning(shadow)
    add_warning(string-plus-int) # clang 8+
    add_warning(undef)
    add_warning(unreachable-code-return)
    add_warning(unreachable-code)
    add_warning(unused-exception-parameter)
    add_warning(unused-macros)
    add_warning(unused-member-function)
    add_warning(zero-as-null-pointer-constant)

    if (WEVERYTHING)
        add_warning(everything)
        no_warning(c++98-compat-pedantic)
        no_warning(c++98-compat)
        no_warning(c99-extensions)
        no_warning(conversion)
        no_warning(ctad-maybe-unsupported) # clang 9+, linux-only
        no_warning(deprecated-dynamic-exception-spec)
        no_warning(disabled-macro-expansion)
        no_warning(documentation-unknown-command)
        no_warning(double-promotion)
        no_warning(exit-time-destructors)
        no_warning(float-equal)
        no_warning(global-constructors)
        no_warning(gnu-anonymous-struct)
        no_warning(missing-prototypes)
        no_warning(missing-variable-declarations)
        no_warning(nested-anon-types)
        no_warning(packed)
        no_warning(padded)
        no_warning(return-std-move-in-c++11) # clang 7+
        no_warning(shift-sign-overflow)
        no_warning(sign-conversion)
        no_warning(switch-enum)
        no_warning(undefined-func-template)
        no_warning(unused-template)
        no_warning(vla-extension)
        no_warning(vla)
        no_warning(weak-template-vtables)
        no_warning(weak-vtables)
        no_warning(zero-length-array)

        # TODO Enable conversion, sign-conversion, double-promotion warnings.
    endif ()
elseif (COMPILER_GCC)
    # Add compiler options only to c++ compiler
    function(add_cxx_compile_options option)
        add_compile_options("$<$<STREQUAL:$<TARGET_PROPERTY:LINKER_LANGUAGE>,CXX>:${option}>")
    endfunction()
    # Warn about boolean expression compared with an integer value different from true/false
    add_cxx_compile_options(-Wbool-compare)
    # Warn whenever a pointer is cast such that the required alignment of the target is increased.
    add_cxx_compile_options(-Wcast-align)
    # Warn whenever a pointer is cast so as to remove a type qualifier from the target type.
    add_cxx_compile_options(-Wcast-qual)
    # Warn when deleting a pointer to incomplete type, which may cause undefined behavior at runtime
    add_cxx_compile_options(-Wdelete-incomplete)
    # Warn if a requested optimization pass is disabled. Code is too big or too complex
    add_cxx_compile_options(-Wdisabled-optimization)
    # Warn about duplicated conditions in an if-else-if chain
    add_cxx_compile_options(-Wduplicated-cond)
    # Warn about a comparison between values of different enumerated types
    add_cxx_compile_options(-Wenum-compare)
    # Warn about uninitialized variables that are initialized with themselves
    add_cxx_compile_options(-Winit-self)
    # Warn about logical not used on the left hand side operand of a comparison
    add_cxx_compile_options(-Wlogical-not-parentheses)
    # Warn about suspicious uses of logical operators in expressions
    add_cxx_compile_options(-Wlogical-op)
    # Warn if there exists a path from the function entry to a use of the variable that is uninitialized.
    add_cxx_compile_options(-Wmaybe-uninitialized)
    # Warn when the indentation of the code does not reflect the block structure
    add_cxx_compile_options(-Wmisleading-indentation)
    # Warn if a global function is defined without a previous declaration - disabled because of build times
    # add_cxx_compile_options(-Wmissing-declarations)
    # Warn if a user-supplied include directory does not exist
    add_cxx_compile_options(-Wmissing-include-dirs)
    # Obvious
    add_cxx_compile_options(-Wnon-virtual-dtor)
    # Obvious
    add_cxx_compile_options(-Wno-return-local-addr)
    # This warning is disabled due to false positives if compiled with libc++: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=90037
    #add_cxx_compile_options(-Wnull-dereference)
    # Obvious
    add_cxx_compile_options(-Wodr)
    # Obvious
    add_cxx_compile_options(-Wold-style-cast)
    # Warn when a function declaration hides virtual functions from a base class
    # add_cxx_compile_options(-Woverloaded-virtual)
    # Warn about placement new expressions with undefined behavior
    add_cxx_compile_options(-Wplacement-new=2)
    # Warn about anything that depends on the “size of” a function type or of void
    add_cxx_compile_options(-Wpointer-arith)
    # Warn if anything is declared more than once in the same scope
    add_cxx_compile_options(-Wredundant-decls)
    # Member initialization reordering
    add_cxx_compile_options(-Wreorder)
    # Obvious
    add_cxx_compile_options(-Wshadow)
    # Warn if left shifting a negative value
    add_cxx_compile_options(-Wshift-negative-value)
    # Warn about a definition of an unsized deallocation function
    add_cxx_compile_options(-Wsized-deallocation)
    # Warn when the sizeof operator is applied to a parameter that is declared as an array in a function definition
    add_cxx_compile_options(-Wsizeof-array-argument)
    # Warn for suspicious length parameters to certain string and memory built-in functions if the argument uses sizeof
    add_cxx_compile_options(-Wsizeof-pointer-memaccess)
    # Warn about overriding virtual functions that are not marked with the override keyword
    add_cxx_compile_options(-Wsuggest-override)
    # Warn whenever a switch statement has an index of boolean type and the case values are outside the range of a boolean type
    add_cxx_compile_options(-Wswitch-bool)
    # Warn if a self-comparison always evaluates to true or false
    add_cxx_compile_options(-Wtautological-compare)
    # Warn about trampolines generated for pointers to nested functions
    add_cxx_compile_options(-Wtrampolines)
    # Obvious
    add_cxx_compile_options(-Wunused)
    # Warn if vector operation is not implemented via SIMD capabilities of the architecture
    add_cxx_compile_options(-Wvector-operation-performance)
endif ()

if (COMPILER_GCC)
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

add_headers_and_sources(clickhouse_common_io src/Common)
add_headers_and_sources(clickhouse_common_io src/Common/HashTable)
add_headers_and_sources(clickhouse_common_io src/IO)
list (REMOVE_ITEM clickhouse_common_io_sources src/Common/malloc.cpp src/Common/new_delete.cpp)

if(USE_RDKAFKA)
    add_headers_and_sources(dbms src/Storages/Kafka)
endif()


list (APPEND clickhouse_common_io_sources ${CONFIG_BUILD})
list (APPEND clickhouse_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

list (APPEND dbms_sources src/Functions/IFunction.cpp src/Functions/FunctionFactory.cpp src/Functions/FunctionHelpers.cpp src/Functions/extractTimeZoneFromFunctionArguments.cpp)
list (APPEND dbms_headers src/Functions/IFunctionImpl.h src/Functions/FunctionFactory.h src/Functions/FunctionHelpers.h src/Functions/extractTimeZoneFromFunctionArguments.h)

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
list (APPEND dbms_sources src/Dictionaries/DictionaryFactory.cpp src/Dictionaries/DictionarySourceFactory.cpp src/Dictionaries/DictionaryStructure.cpp  src/Dictionaries/getDictionaryConfigurationFromAST.cpp)
list (APPEND dbms_headers src/Dictionaries/DictionaryFactory.h   src/Dictionaries/DictionarySourceFactory.h   src/Dictionaries/DictionaryStructure.h src/Dictionaries/getDictionaryConfigurationFromAST.h)

if (NOT ENABLE_SSL)
    list (REMOVE_ITEM clickhouse_common_io_sources src/Common/OpenSSLHelpers.cpp)
    list (REMOVE_ITEM clickhouse_common_io_headers src/Common/OpenSSLHelpers.h)
endif ()

add_library(clickhouse_common_io ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

add_library (clickhouse_malloc OBJECT src/Common/malloc.cpp)
set_source_files_properties(src/Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

add_library (clickhouse_new_delete STATIC src/Common/new_delete.cpp)
target_link_libraries (clickhouse_new_delete PRIVATE clickhouse_common_io)

if (OS_FREEBSD)
    target_compile_definitions (clickhouse_common_io PUBLIC CLOCK_MONOTONIC_COARSE=CLOCK_MONOTONIC_FAST)
endif ()

add_subdirectory(src/Common/ZooKeeper)
add_subdirectory(src/Common/Config)

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

add_object_library(clickhouse_access src/Access)
add_object_library(clickhouse_core src/Core)
add_object_library(clickhouse_compression src/Compression)
add_object_library(clickhouse_datastreams src/DataStreams)
add_object_library(clickhouse_datatypes src/DataTypes)
add_object_library(clickhouse_databases src/Databases)
add_object_library(clickhouse_disks src/Disks)
add_object_library(clickhouse_interpreters src/Interpreters)
add_object_library(clickhouse_interpreters_clusterproxy src/Interpreters/ClusterProxy)
add_object_library(clickhouse_columns src/Columns)
add_object_library(clickhouse_storages src/Storages)
add_object_library(clickhouse_storages_distributed src/Storages/Distributed)
add_object_library(clickhouse_storages_mergetree src/Storages/MergeTree)
add_object_library(clickhouse_storages_liveview src/Storages/LiveView)
add_object_library(clickhouse_client src/Client)
add_object_library(clickhouse_formats src/Formats)
add_object_library(clickhouse_processors src/Processors)
add_object_library(clickhouse_processors_executors src/Processors/Executors)
add_object_library(clickhouse_processors_formats src/Processors/Formats)
add_object_library(clickhouse_processors_formats_impl src/Processors/Formats/Impl)
add_object_library(clickhouse_processors_transforms src/Processors/Transforms)
add_object_library(clickhouse_processors_sources src/Processors/Sources)


if (MAKE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    add_library (dbms STATIC ${dbms_headers} ${dbms_sources})
    set (all_modules dbms)
else()
    add_library (dbms SHARED ${dbms_headers} ${dbms_sources})
    target_link_libraries (dbms PUBLIC ${all_modules})
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

# Otherwise it will slow down stack traces printing too much.
set_source_files_properties(
        src/Common/Elf.cpp
        src/Common/Dwarf.cpp
        src/Common/SymbolIndex.cpp
        PROPERTIES COMPILE_FLAGS "-O3 ${WITHOUT_COVERAGE}")

target_link_libraries (clickhouse_common_io
        PUBLIC
    common
        PRIVATE
    string_utils
    widechar_width
    ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
    ${DOUBLE_CONVERSION_LIBRARIES}
    ryu
        PUBLIC
    ${Poco_Net_LIBRARY}
    ${Poco_Util_LIBRARY}
    ${Poco_Foundation_LIBRARY}
    ${Poco_XML_LIBRARY}
)

if(RE2_LIBRARY)
    target_link_libraries(clickhouse_common_io PUBLIC ${RE2_LIBRARY})
endif()
if(RE2_ST_LIBRARY)
    target_link_libraries(clickhouse_common_io PUBLIC ${RE2_ST_LIBRARY})
endif()

target_link_libraries(clickhouse_common_io
        PUBLIC
    ${CITYHASH_LIBRARIES}
        PRIVATE
    ${Poco_XML_LIBRARY}
    ${ZLIB_LIBRARIES}
    ${EXECINFO_LIBRARIES}
        PUBLIC
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
        PUBLIC
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

if(CPUID_LIBRARY)
    target_link_libraries(clickhouse_common_io PRIVATE ${CPUID_LIBRARY})
endif()

if(CPUINFO_LIBRARY)
    target_link_libraries(clickhouse_common_io PRIVATE ${CPUINFO_LIBRARY})
endif()

dbms_target_link_libraries (
        PRIVATE
    clickhouse_parsers
    clickhouse_common_config
    clickhouse_common_zookeeper
    string_utils # FIXME: not sure if it's private
        PUBLIC
    clickhouse_common_io
        PRIVATE
    clickhouse_dictionaries_embedded
    ${LZ4_LIBRARY}
        PUBLIC
    ${MYSQLXX_LIBRARY}
        PRIVATE
    ${BTRIE_LIBRARIES}
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    ${Boost_FILESYSTEM_LIBRARY}
        PUBLIC
    ${Boost_SYSTEM_LIBRARY}
)

target_include_directories(clickhouse_common_io PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/src/Core/include) # uses some includes from core
dbms_target_include_directories(PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/src/Core/include)

target_include_directories(clickhouse_common_io SYSTEM PUBLIC ${PCG_RANDOM_INCLUDE_DIR})
dbms_target_include_directories(SYSTEM PUBLIC ${PCG_RANDOM_INCLUDE_DIR})

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

if (Poco_SQL_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
    dbms_target_include_directories (SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
endif()

if (USE_POCO_SQLODBC)
    target_link_libraries (clickhouse_common_io PRIVATE ${Poco_SQL_LIBRARY})
    dbms_target_link_libraries (PRIVATE ${Poco_SQLODBC_LIBRARY} ${Poco_SQL_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_SQL_INCLUDE_DIR})
        dbms_target_include_directories (SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_SQLODBC_INCLUDE_DIR} SYSTEM PUBLIC ${Poco_SQL_INCLUDE_DIR})
    endif()
endif()

if (Poco_Data_FOUND)
    target_include_directories (clickhouse_common_io SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
    dbms_target_include_directories (SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
endif()

if (USE_POCO_DATAODBC)
    target_link_libraries (clickhouse_common_io PRIVATE ${Poco_Data_LIBRARY})
    dbms_target_link_libraries (PRIVATE ${Poco_DataODBC_LIBRARY})
    if (NOT USE_INTERNAL_POCO_LIBRARY)
        dbms_target_include_directories (SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_DataODBC_INCLUDE_DIR})
    endif()
endif()

if (USE_POCO_MONGODB)
    dbms_target_link_libraries (PRIVATE ${Poco_MongoDB_LIBRARY})
endif()

if (USE_POCO_REDIS)
    dbms_target_link_libraries (PRIVATE ${Poco_Redis_LIBRARY})
endif()

if (USE_POCO_NETSSL)
    target_link_libraries (clickhouse_common_io PRIVATE ${Poco_NetSSL_LIBRARY} ${Poco_Crypto_LIBRARY})
    dbms_target_link_libraries (PRIVATE ${Poco_NetSSL_LIBRARY} ${Poco_Crypto_LIBRARY})
endif()

if (USE_POCO_JSON)
    dbms_target_link_libraries (PRIVATE ${Poco_JSON_LIBRARY})
endif()

dbms_target_link_libraries (PRIVATE ${Poco_Foundation_LIBRARY})

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

if (USE_JEMALLOC)
    dbms_target_include_directories (SYSTEM BEFORE PRIVATE ${JEMALLOC_INCLUDE_DIR}) # used in Interpreters/AsynchronousMetrics.cpp
    target_include_directories (clickhouse_new_delete SYSTEM BEFORE PRIVATE ${JEMALLOC_INCLUDE_DIR})

    if(NOT MAKE_STATIC_LIBRARIES AND ${JEMALLOC_LIBRARIES} MATCHES "${CMAKE_STATIC_LIBRARY_SUFFIX}$")
        # mallctl in dbms/src/Interpreters/AsynchronousMetrics.cpp
        # Actually we link JEMALLOC to almost all libraries.
        # This is just hotfix for some uninvestigated problem.
        target_link_libraries(clickhouse_interpreters PRIVATE ${JEMALLOC_LIBRARIES})
    endif()
endif ()

dbms_target_include_directories (PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_common_io PUBLIC ${DBMS_INCLUDE_DIR})

target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${DOUBLE_CONVERSION_INCLUDE_DIR})

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
