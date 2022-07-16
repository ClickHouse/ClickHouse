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
if(USE_STATIC_LIBRARIES)
    set(MAX_LINKER_MEMORY 3500)
else()
    set(MAX_LINKER_MEMORY 2500)
endif()
include(../cmake/limit_jobs.cmake)

set (CONFIG_VERSION "${CMAKE_CURRENT_BINARY_DIR}/Common/config_version.h")
set (CONFIG_COMMON "${CMAKE_CURRENT_BINARY_DIR}/Common/config.h")

include (../cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL} revision ${VERSION_REVISION} ${VERSION_OFFICIAL}")
include (configure_config.cmake)
configure_file (Common/config.h.in ${CONFIG_COMMON})
configure_file (Common/config_version.h.in ${CONFIG_VERSION})
configure_file (Core/config_core.h.in "${CMAKE_CURRENT_BINARY_DIR}/Core/include/config_core.h")

if (USE_DEBUG_HELPERS)
    get_target_property(MAGIC_ENUM_INCLUDE_DIR ch_contrib::magic_enum INTERFACE_INCLUDE_DIRECTORIES)
    # CMake generator expression will do insane quoting when it encounters special character like quotes, spaces, etc.
    # Prefixing "SHELL:" will force it to use the original text.
    set (INCLUDE_DEBUG_HELPERS "SHELL:-I\"${ClickHouse_SOURCE_DIR}/base\" -I\"${MAGIC_ENUM_INCLUDE_DIR}\" -include \"${ClickHouse_SOURCE_DIR}/src/Core/iostream_debug_helpers.h\"")
    # Use generator expression as we don't want to pollute CMAKE_CXX_FLAGS, which will interfere with CMake check system.
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:${INCLUDE_DEBUG_HELPERS}>)
endif ()

# ClickHouse developers may use platform-dependent code under some macro (e.g. `#ifdef ENABLE_MULTITARGET`).
# If turned ON, this option defines such macro.
# See `src/Common/TargetSpecific.h`
option(ENABLE_MULTITARGET_CODE "Enable platform-dependent code" ON)

if (ENABLE_MULTITARGET_CODE)
    add_definitions(-DENABLE_MULTITARGET_CODE=1)
else()
    add_definitions(-DENABLE_MULTITARGET_CODE=0)
endif()


add_subdirectory (Access)
add_subdirectory (Backups)
add_subdirectory (Columns)
add_subdirectory (Common)
add_subdirectory (Core)
add_subdirectory (QueryPipeline)
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
add_subdirectory (Coordination)
add_subdirectory (Bridge)
add_subdirectory (Daemon)
add_subdirectory (Loggers)


set(dbms_headers)
set(dbms_sources)

add_headers_and_sources(clickhouse_common_io Common)
add_headers_and_sources(clickhouse_common_io Common/HashTable)
add_headers_and_sources(clickhouse_common_io IO)
add_headers_and_sources(clickhouse_common_io IO/Archives)
add_headers_and_sources(clickhouse_common_io IO/S3)
list (REMOVE_ITEM clickhouse_common_io_sources Common/malloc.cpp Common/new_delete.cpp)

add_headers_and_sources(dbms Disks/IO)
add_headers_and_sources(dbms Disks/ObjectStorages)
if (TARGET ch_contrib::sqlite)
    add_headers_and_sources(dbms Databases/SQLite)
endif()

if (TARGET ch_contrib::rdkafka)
    add_headers_and_sources(dbms Storages/Kafka)
endif()

if (TARGET ch_contrib::nats_io)
    add_headers_and_sources(dbms Storages/NATS)
endif()

add_headers_and_sources(dbms Storages/MeiliSearch)

if (TARGET ch_contrib::amqp_cpp)
    add_headers_and_sources(dbms Storages/RabbitMQ)
endif()

if (USE_LIBPQXX)
    add_headers_and_sources(dbms Core/PostgreSQL)
    add_headers_and_sources(dbms Databases/PostgreSQL)
    add_headers_and_sources(dbms Storages/PostgreSQL)
endif()

if (TARGET ch_contrib::rocksdb)
    add_headers_and_sources(dbms Storages/RocksDB)
endif()

if (TARGET ch_contrib::aws_s3)
    add_headers_and_sources(dbms Common/S3)
    add_headers_and_sources(dbms Disks/ObjectStorages/S3)
endif()

if (TARGET ch_contrib::azure_sdk)
    add_headers_and_sources(dbms Disks/ObjectStorages/AzureBlobStorage)
endif()

if (TARGET ch_contrib::hdfs)
    add_headers_and_sources(dbms Storages/HDFS)
    add_headers_and_sources(dbms Disks/ObjectStorages/HDFS)
endif()

add_headers_and_sources(dbms Disks/ObjectStorages/Cached)
add_headers_and_sources(dbms Disks/ObjectStorages/Web)

add_headers_and_sources(dbms Storages/Cache)
if (TARGET ch_contrib::hivemetastore)
    add_headers_and_sources(dbms Storages/Hive)
endif()

if (OS_LINUX)
    add_headers_and_sources(dbms Storages/FileLog)
else()
    message(STATUS "StorageFileLog is only supported on Linux")
endif ()

list (APPEND clickhouse_common_io_sources ${CONFIG_BUILD})
list (APPEND clickhouse_common_io_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

list (APPEND dbms_sources Functions/IFunction.cpp Functions/FunctionFactory.cpp Functions/FunctionHelpers.cpp Functions/extractTimeZoneFromFunctionArguments.cpp Functions/replicate.cpp Functions/FunctionsLogical.cpp)
list (APPEND dbms_headers Functions/IFunction.h Functions/FunctionFactory.h Functions/FunctionHelpers.h Functions/extractTimeZoneFromFunctionArguments.h Functions/replicate.h Functions/FunctionsLogical.h)

list (APPEND dbms_sources
    AggregateFunctions/IAggregateFunction.cpp
    AggregateFunctions/AggregateFunctionFactory.cpp
    AggregateFunctions/AggregateFunctionCombinatorFactory.cpp
    AggregateFunctions/AggregateFunctionState.cpp
    AggregateFunctions/AggregateFunctionCount.cpp
    AggregateFunctions/parseAggregateFunctionParameters.cpp)
list (APPEND dbms_headers
    AggregateFunctions/IAggregateFunction.h
    AggregateFunctions/IAggregateFunctionCombinator.h
    AggregateFunctions/AggregateFunctionFactory.h
    AggregateFunctions/AggregateFunctionCombinatorFactory.h
    AggregateFunctions/AggregateFunctionState.h
    AggregateFunctions/AggregateFunctionCount.cpp
    AggregateFunctions/FactoryHelpers.h
    AggregateFunctions/parseAggregateFunctionParameters.h)

list (APPEND dbms_sources
    TableFunctions/ITableFunction.cpp
    TableFunctions/TableFunctionView.cpp
    TableFunctions/TableFunctionFactory.cpp)
list (APPEND dbms_headers
    TableFunctions/ITableFunction.h
    TableFunctions/TableFunctionView.h
    TableFunctions/TableFunctionFactory.h)

list (APPEND dbms_sources
    Dictionaries/DictionaryFactory.cpp
    Dictionaries/DictionarySourceFactory.cpp
    Dictionaries/DictionaryStructure.cpp
    Dictionaries/getDictionaryConfigurationFromAST.cpp)
list (APPEND dbms_headers
    Dictionaries/DictionaryFactory.h
    Dictionaries/DictionarySourceFactory.h
    Dictionaries/DictionaryStructure.h
    Dictionaries/getDictionaryConfigurationFromAST.h)

if (NOT ENABLE_SSL)
    list (REMOVE_ITEM clickhouse_common_io_sources Common/OpenSSLHelpers.cpp)
    list (REMOVE_ITEM clickhouse_common_io_headers Common/OpenSSLHelpers.h)
endif ()

add_library(clickhouse_common_io ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

if (SPLIT_SHARED_LIBRARIES)
    target_compile_definitions(clickhouse_common_io PRIVATE SPLIT_SHARED_LIBRARIES)
endif ()

add_library (clickhouse_malloc OBJECT Common/malloc.cpp)
set_source_files_properties(Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

if (((SANITIZE STREQUAL "thread") OR (SANITIZE STREQUAL "address")) AND COMPILER_GCC)
    message(WARNING "Memory tracking is disabled, due to gcc sanitizers")
else()
    add_library (clickhouse_new_delete STATIC Common/new_delete.cpp)
    target_link_libraries (clickhouse_new_delete PRIVATE clickhouse_common_io)
    if (TARGET ch_contrib::jemalloc)
        target_link_libraries (clickhouse_new_delete PRIVATE ch_contrib::jemalloc)
    endif()
endif()

if (TARGET ch_contrib::jemalloc)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::jemalloc)
endif()

add_subdirectory(Access/Common)
add_subdirectory(Common/ZooKeeper)
add_subdirectory(Common/Config)

set (all_modules)
macro(add_object_library name common_path)
    if (USE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
        add_headers_and_sources(dbms ${common_path})
    else ()
        list (APPEND all_modules ${name})
        add_headers_and_sources(${name} ${common_path})
        add_library(${name} SHARED ${${name}_sources} ${${name}_headers})
        if (OS_DARWIN)
            target_link_libraries (${name} PRIVATE -Wl,-undefined,dynamic_lookup)
        else()
            target_link_libraries (${name} PRIVATE -Wl,--unresolved-symbols=ignore-all)
        endif()
    endif ()
endmacro()

add_object_library(clickhouse_access Access)
add_object_library(clickhouse_backups Backups)
add_object_library(clickhouse_core Core)
add_object_library(clickhouse_core_mysql Core/MySQL)
add_object_library(clickhouse_compression Compression)
add_object_library(clickhouse_querypipeline QueryPipeline)
add_object_library(clickhouse_datatypes DataTypes)
add_object_library(clickhouse_datatypes_serializations DataTypes/Serializations)
add_object_library(clickhouse_databases Databases)
add_object_library(clickhouse_databases_mysql Databases/MySQL)
add_object_library(clickhouse_disks Disks)
add_object_library(clickhouse_interpreters Interpreters)
add_object_library(clickhouse_interpreters_access Interpreters/Access)
add_object_library(clickhouse_interpreters_mysql Interpreters/MySQL)
add_object_library(clickhouse_interpreters_clusterproxy Interpreters/ClusterProxy)
add_object_library(clickhouse_interpreters_jit Interpreters/JIT)
add_object_library(clickhouse_columns Columns)
add_object_library(clickhouse_storages Storages)
add_object_library(clickhouse_storages_mysql Storages/MySQL)
add_object_library(clickhouse_storages_distributed Storages/Distributed)
add_object_library(clickhouse_storages_mergetree Storages/MergeTree)
add_object_library(clickhouse_storages_liveview Storages/LiveView)
add_object_library(clickhouse_storages_windowview Storages/WindowView)
add_object_library(clickhouse_client Client)
add_object_library(clickhouse_bridge BridgeHelper)
add_object_library(clickhouse_server Server)
add_object_library(clickhouse_server_http Server/HTTP)
add_object_library(clickhouse_formats Formats)
add_object_library(clickhouse_processors Processors)
add_object_library(clickhouse_processors_executors Processors/Executors)
add_object_library(clickhouse_processors_formats Processors/Formats)
add_object_library(clickhouse_processors_formats_impl Processors/Formats/Impl)
add_object_library(clickhouse_processors_transforms Processors/Transforms)
add_object_library(clickhouse_processors_sources Processors/Sources)
add_object_library(clickhouse_processors_sinks Processors/Sinks)
add_object_library(clickhouse_processors_merges Processors/Merges)
add_object_library(clickhouse_processors_ttl Processors/TTL)
add_object_library(clickhouse_processors_merges_algorithms Processors/Merges/Algorithms)
add_object_library(clickhouse_processors_queryplan Processors/QueryPlan)
add_object_library(clickhouse_processors_queryplan_optimizations Processors/QueryPlan/Optimizations)

if (TARGET ch_contrib::nuraft)
    add_object_library(clickhouse_coordination Coordination)
endif()

if (USE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    add_library (dbms STATIC ${dbms_headers} ${dbms_sources})
    target_link_libraries (dbms PRIVATE ch_contrib::libdivide)
    if (TARGET ch_contrib::jemalloc)
        target_link_libraries (dbms PRIVATE ch_contrib::jemalloc)
    endif()
    set (all_modules dbms)
else()
    add_library (dbms SHARED ${dbms_headers} ${dbms_sources})
    target_link_libraries (dbms PUBLIC ${all_modules})
    target_link_libraries (clickhouse_interpreters PRIVATE ch_contrib::libdivide)
    if (TARGET ch_contrib::jemalloc)
        target_link_libraries (clickhouse_interpreters PRIVATE ch_contrib::jemalloc)
    endif()
    list (APPEND all_modules dbms)
    # force all split libs to be linked
    if (OS_DARWIN)
        set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-undefined,error")
    else()
        set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-as-needed")
    endif()
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

dbms_target_include_directories (PUBLIC "${ClickHouse_SOURCE_DIR}/src" "${ClickHouse_BINARY_DIR}/src")
target_include_directories (clickhouse_common_io PUBLIC "${ClickHouse_SOURCE_DIR}/src" "${ClickHouse_BINARY_DIR}/src")

if (TARGET ch_contrib::llvm)
    dbms_target_link_libraries (PUBLIC ch_contrib::llvm)
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
            ch_contrib::double_conversion
            ch_contrib::dragonbox_to_chars
)

# Use X86 AVX2/AVX512 instructions to accelerate filter operations
set_source_files_properties(
        Columns/ColumnFixedString.cpp
        Columns/ColumnsCommon.cpp
        Columns/ColumnVector.cpp
        Columns/ColumnDecimal.cpp
        Columns/ColumnString.cpp
        PROPERTIES COMPILE_FLAGS "${X86_INTRINSICS_FLAGS}")

target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::re2_st)
target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::re2)

target_link_libraries(clickhouse_common_io
        PRIVATE
            ${EXECINFO_LIBRARIES}
        PUBLIC
            boost::program_options
            boost::system
            ch_contrib::cityhash
            ch_contrib::zlib
            pcg_random
            Poco::Foundation
)

if (TARGET ch_contrib::cpuid)
    target_link_libraries(clickhouse_common_io PRIVATE ch_contrib::cpuid)
endif()

dbms_target_link_libraries(PUBLIC ch_contrib::abseil_swiss_tables)

# Make dbms depend on roaring instead of clickhouse_common_io so that roaring itself can depend on clickhouse_common_io
# That way we we can redirect malloc/free functions avoiding circular dependencies
dbms_target_link_libraries(PUBLIC ch_contrib::roaring)

if (TARGET ch_contrib::rdkafka)
    dbms_target_link_libraries(PRIVATE ch_contrib::rdkafka ch_contrib::cppkafka)
endif()

if (TARGET ch_contrib::nats_io)
    dbms_target_link_libraries(PRIVATE ch_contrib::nats_io)
endif()

if (TARGET ch_contrib::sasl2)
    dbms_target_link_libraries(PRIVATE ch_contrib::sasl2)
endif()

if (TARGET ch_contrib::krb5)
    dbms_target_link_libraries(PRIVATE ch_contrib::krb5)
endif()

if (TARGET ch_contrib::nuraft)
    dbms_target_link_libraries(PUBLIC ch_contrib::nuraft)
endif()

dbms_target_link_libraries (
    PRIVATE
        boost::filesystem
        boost::program_options
        clickhouse_common_config
        clickhouse_common_zookeeper
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        ch_contrib::lz4
        Poco::JSON
        string_utils
    PUBLIC
        boost::system
        clickhouse_common_io
        Poco::MongoDB
)

if (TARGET ch::mysqlxx)
    dbms_target_link_libraries (PUBLIC ch::mysqlxx)
endif()

dbms_target_link_libraries (
    PUBLIC
        boost::circular_buffer
        boost::heap)

target_include_directories(clickhouse_common_io PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/Core/include") # uses some includes from core
dbms_target_include_directories(PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/Core/include")

target_link_libraries(clickhouse_common_io PUBLIC
    ch_contrib::miniselect
    ch_contrib::pdqsort)
dbms_target_link_libraries(PUBLIC
    ch_contrib::miniselect
    ch_contrib::pdqsort)

dbms_target_link_libraries(PRIVATE ch_contrib::zstd)
target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::zstd)
target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::xz)

if (TARGET ch_contrib::icu)
    dbms_target_link_libraries (PRIVATE ch_contrib::icu)
endif ()

if (TARGET ch_contrib::capnp)
    dbms_target_link_libraries (PRIVATE ch_contrib::capnp)
endif ()

if (TARGET ch_contrib::parquet)
    dbms_target_link_libraries (PRIVATE ch_contrib::parquet)
endif ()

if (TARGET ch_contrib::avrocpp)
    dbms_target_link_libraries(PRIVATE ch_contrib::avrocpp)
endif ()

if (TARGET OpenSSL::Crypto)
    dbms_target_link_libraries (PRIVATE OpenSSL::Crypto)
    target_link_libraries (clickhouse_common_io PRIVATE OpenSSL::Crypto)
endif ()

if (TARGET ch_contrib::ldap)
    dbms_target_link_libraries (PRIVATE ch_contrib::ldap ch_contrib::lber)
endif ()
dbms_target_link_libraries (PRIVATE ch_contrib::sparsehash)

if (TARGET ch_contrib::protobuf)
    dbms_target_link_libraries (PRIVATE ch_contrib::protobuf)
endif ()

if (TARGET clickhouse_grpc_protos)
    dbms_target_link_libraries (PUBLIC clickhouse_grpc_protos)
endif()

if (TARGET ch_contrib::hdfs)
    dbms_target_link_libraries(PRIVATE ch_contrib::hdfs)
endif()

if (TARGET ch_contrib::hivemetastore)
    dbms_target_link_libraries(PRIVATE ch_contrib::hivemetastore)
endif()


if (TARGET ch_contrib::aws_s3)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::aws_s3)
endif()

if (TARGET ch_contrib::azure_sdk)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::azure_sdk)
endif()

if (TARGET ch_contrib::s2)
    dbms_target_link_libraries (PUBLIC ch_contrib::s2)
endif()

if (TARGET ch_contrib::brotli)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::brotli)
endif()

if (TARGET ch_contrib::snappy)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::snappy)
endif()

if (TARGET ch_contrib::amqp_cpp)
    dbms_target_link_libraries(PUBLIC ch_contrib::amqp_cpp)
endif()

if (TARGET ch_contrib::sqlite)
    dbms_target_link_libraries(PUBLIC ch_contrib::sqlite)
endif()

if (TARGET ch_contrib::msgpack)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::msgpack)
endif()

target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::fast_float)

if (USE_ORC)
    dbms_target_link_libraries(PUBLIC ${ORC_LIBRARIES})
    dbms_target_include_directories(SYSTEM BEFORE PUBLIC ${ORC_INCLUDE_DIR} "${CMAKE_BINARY_DIR}/contrib/orc/c++/include")
endif ()

if (TARGET ch_contrib::rocksdb)
    dbms_target_link_libraries(PUBLIC ch_contrib::rocksdb)
endif()

if (TARGET ch_contrib::libpqxx)
    dbms_target_link_libraries(PUBLIC ch_contrib::libpqxx)
endif()

if (TARGET ch_contrib::datasketches)
    target_link_libraries (clickhouse_aggregate_functions PRIVATE ch_contrib::datasketches)
endif ()

target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::lz4)

dbms_target_link_libraries(PRIVATE _boost_context)

if (ENABLE_NLP)
    dbms_target_link_libraries (PUBLIC ch_contrib::stemmer)
    dbms_target_link_libraries (PUBLIC ch_contrib::wnb)
    dbms_target_link_libraries (PUBLIC ch_contrib::lemmagen)
    dbms_target_link_libraries (PUBLIC ch_contrib::nlp_data)
endif()

if (TARGET ch_contrib::bzip2)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::bzip2)
endif()

if (TARGET ch_contrib::minizip)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::minizip)
endif ()

if (TARGET ch_contrib::simdjson)
    dbms_target_link_libraries(PRIVATE ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    dbms_target_link_libraries(PRIVATE ch_contrib::rapidjson)
endif()

dbms_target_link_libraries(PUBLIC ch_contrib::consistent_hashing)

include ("${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake")

if (ENABLE_TESTS)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" CONFIGURE_DEPENDS RELATIVE "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources("${ClickHouse_SOURCE_DIR}/src" dbms_gtest_sources)
    clickhouse_add_executable(unit_tests_dbms ${dbms_gtest_sources})

    # gtest framework has substandard code
    target_compile_options(unit_tests_dbms PRIVATE
        -Wno-zero-as-null-pointer-constant
        -Wno-covered-switch-default
        -Wno-undef
        -Wno-sign-compare
        -Wno-used-but-marked-unused
        -Wno-missing-noreturn
        -Wno-gnu-zero-variadic-macro-arguments
    )

    target_link_libraries(unit_tests_dbms PRIVATE
        ch_contrib::gtest_all
        clickhouse_functions
        clickhouse_aggregate_functions
        clickhouse_parsers
        clickhouse_storages_system
        dbms
        clickhouse_common_config
        clickhouse_common_zookeeper
        string_utils)

    if (TARGET ch_contrib::simdjson)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::simdjson)
    endif()

    if (TARGET ch_contrib::yaml_cpp)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::yaml_cpp)
    endif()

    add_check(unit_tests_dbms)
endif ()
