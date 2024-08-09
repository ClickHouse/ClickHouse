add_compile_options($<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:${COVERAGE_FLAGS}>)

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
set(MAX_LINKER_MEMORY 3500)
include(../cmake/limit_jobs.cmake)

include (../cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL} revision ${VERSION_REVISION} ${VERSION_OFFICIAL}")
include (configure_config.cmake)
configure_file (Common/config.h.in ${CONFIG_INCLUDE_PATH}/config.h)
configure_file (Common/config_version.cpp.in ${CONFIG_INCLUDE_PATH}/config_version.cpp)

# ClickHouse developers may use platform-dependent code under some macro (e.g. `#ifdef ENABLE_MULTITARGET`).
# If turned ON, this option defines such macro.
# See `src/Common/TargetSpecific.h`
option(ENABLE_MULTITARGET_CODE "Enable platform-dependent code" ON)

if (NO_SSE3_OR_HIGHER)
    # Optimized x86 code in DECLARE_*_SPECIFIC_CODE blocks (see `src/Common/TargetSpecific.h`) is sometimes marked FORCE_INLINE. As a
    # result, its instruction set requirements (e.g. SSE4.2) leak into generic code. This is normally not a problem for standard x86 builds
    # because generic code is compiled with SSE 4.2 anyways. But it breaks SSE2-only builds. Therefore disabling the multitarget code
    # machinery and always use generic code. (The cleaner alternative is removing FORCE_INLINE but that impacts performance too much.)
    set(ENABLE_MULTITARGET_CODE OFF)
endif()

if (ENABLE_MULTITARGET_CODE)
    add_definitions(-DENABLE_MULTITARGET_CODE=1)
else()
    add_definitions(-DENABLE_MULTITARGET_CODE=0)
endif()

set(dbms_headers)
set(dbms_sources)

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
add_subdirectory (Analyzer)
add_subdirectory (Planner)
add_subdirectory (Interpreters)
add_subdirectory (AggregateFunctions)
add_subdirectory (Client)
add_subdirectory (TableFunctions)
add_subdirectory (Processors)
add_subdirectory (Compression)
add_subdirectory (Server)
add_subdirectory (Coordination)
add_subdirectory (Bridge)
add_subdirectory (Daemon)
add_subdirectory (Loggers)
add_subdirectory (Formats)

add_headers_and_sources(clickhouse_common_io Common)
add_headers_and_sources(clickhouse_common_io Common/HashTable)
add_headers_and_sources(clickhouse_common_io Common/Scheduler)
add_headers_and_sources(clickhouse_common_io Common/Scheduler/Nodes)
add_headers_and_sources(clickhouse_common_io IO)
add_headers_and_sources(clickhouse_common_io IO/Archives)
add_headers_and_sources(clickhouse_common_io IO/Protobuf)
add_headers_and_sources(clickhouse_common_io IO/S3)
add_headers_and_sources(clickhouse_common_io IO/AzureBlobStorage)
list (REMOVE_ITEM clickhouse_common_io_sources Common/malloc.cpp Common/new_delete.cpp)


add_headers_and_sources(clickhouse_compression Compression)
add_headers_and_sources(clickhouse_compression Parsers)
add_headers_and_sources(clickhouse_compression Core)
#Included these specific files to avoid linking grpc
add_glob(clickhouse_compression_headers Server/ServerType.h)
add_glob(clickhouse_compression_sources Server/ServerType.cpp)
add_library(clickhouse_compression ${clickhouse_compression_headers} ${clickhouse_compression_sources})


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

add_headers_and_sources(dbms Storages/ObjectStorage)
add_headers_and_sources(dbms Storages/ObjectStorage/Azure)
add_headers_and_sources(dbms Storages/ObjectStorage/S3)
add_headers_and_sources(dbms Storages/ObjectStorage/HDFS)
add_headers_and_sources(dbms Storages/ObjectStorage/DataLakes)
add_headers_and_sources(dbms Common/NamedCollections)

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
    add_headers_and_sources(dbms IO/AzureBlobStorage)
endif()

if (TARGET ch_contrib::hdfs)
    add_headers_and_sources(dbms Disks/ObjectStorages/HDFS)
endif()

add_headers_and_sources(dbms Disks/ObjectStorages/Cached)
add_headers_and_sources(dbms Disks/ObjectStorages/Local)
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

list(APPEND clickhouse_common_io_sources ${CONFIG_INCLUDE_PATH}/config_version.cpp)

if (NOT ENABLE_SSL)
    list (REMOVE_ITEM clickhouse_common_io_sources Common/OpenSSLHelpers.cpp)
    list (REMOVE_ITEM clickhouse_common_io_headers Common/OpenSSLHelpers.h)
endif ()

add_library(clickhouse_common_io ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

set_source_files_properties(Common/ThreadFuzzer.cpp PROPERTIES COMPILE_FLAGS "-fomit-frame-pointer -momit-leaf-frame-pointer")

add_library (clickhouse_malloc OBJECT Common/malloc.cpp)
set_source_files_properties(Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

add_library (clickhouse_new_delete STATIC Common/new_delete.cpp)
target_link_libraries (clickhouse_new_delete PRIVATE clickhouse_common_io)
if (TARGET ch_contrib::jemalloc)
    target_link_libraries (clickhouse_new_delete PRIVATE ch_contrib::jemalloc)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::jemalloc)
    target_link_libraries (clickhouse_storages_system PRIVATE ch_contrib::jemalloc)
endif()

if (TARGET ch_contrib::azure_sdk)
    target_link_libraries(clickhouse_storages_system PRIVATE ch_contrib::azure_sdk)
endif()

target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::sparsehash ch_contrib::incbin)

add_subdirectory(Access/Common)
add_subdirectory(Common/ZooKeeper)
add_subdirectory(Common/Config)

set (all_modules)
macro(add_object_library name common_path)
    add_headers_and_sources(dbms ${common_path})
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
add_object_library(clickhouse_analyzer Analyzer)
add_object_library(clickhouse_analyzer_passes Analyzer/Passes)
add_object_library(clickhouse_analyzer_passes Analyzer/Resolve)
add_object_library(clickhouse_planner Planner)
add_object_library(clickhouse_interpreters Interpreters)
add_object_library(clickhouse_interpreters_cache Interpreters/Cache)
add_object_library(clickhouse_interpreters_hash_join Interpreters/HashJoin)
add_object_library(clickhouse_interpreters_access Interpreters/Access)
add_object_library(clickhouse_interpreters_mysql Interpreters/MySQL)
add_object_library(clickhouse_interpreters_clusterproxy Interpreters/ClusterProxy)
add_object_library(clickhouse_interpreters_jit Interpreters/JIT)
add_object_library(clickhouse_columns Columns)
add_object_library(clickhouse_storages Storages)
add_object_library(clickhouse_storages_mysql Storages/MySQL)
add_object_library(clickhouse_storages_distributed Storages/Distributed)
add_object_library(clickhouse_storages_mergetree Storages/MergeTree)
add_object_library(clickhouse_storages_statistics Storages/Statistics)
add_object_library(clickhouse_storages_liveview Storages/LiveView)
add_object_library(clickhouse_storages_windowview Storages/WindowView)
add_object_library(clickhouse_storages_s3queue Storages/ObjectStorageQueue)
add_object_library(clickhouse_storages_materializedview Storages/MaterializedView)
add_object_library(clickhouse_storages_time_series Storages/TimeSeries)
add_object_library(clickhouse_client Client)
# Always compile this file with the highest possible level of optimizations, even in Debug builds.
# https://github.com/ClickHouse/ClickHouse/issues/65745
set_source_files_properties(Client/ClientBaseOptimizedParts.cpp PROPERTIES COMPILE_FLAGS "-O3")
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
add_object_library(clickhouse_user_defined_functions Functions/UserDefined)

if (USE_PARQUET)
    add_object_library(clickhouse_processors_formats_impl_parquet Processors/Formats/Impl/Parquet)
endif()

if (TARGET ch_contrib::nuraft)
    add_object_library(clickhouse_coordination Coordination)
endif()

add_library (dbms STATIC ${dbms_headers} ${dbms_sources})
target_link_libraries (dbms PRIVATE ch_contrib::libdivide)
if (TARGET ch_contrib::jemalloc)
    target_link_libraries (dbms PRIVATE ch_contrib::jemalloc)
endif()
set (all_modules dbms)

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

macro (dbms_target_include_directories)
    foreach (module ${all_modules})
        target_include_directories (${module} ${ARGN})
    endforeach ()
endmacro ()

dbms_target_include_directories (PUBLIC "${ClickHouse_SOURCE_DIR}/src" "${ClickHouse_BINARY_DIR}/src")
target_include_directories (clickhouse_common_io PUBLIC "${ClickHouse_SOURCE_DIR}/src" "${ClickHouse_BINARY_DIR}/src")

if (TARGET ch_contrib::llvm)
    dbms_target_link_libraries (PUBLIC ch_contrib::llvm)
    target_link_libraries (clickhouse_compression PUBLIC ch_contrib::llvm)
endif ()

if (TARGET ch_contrib::gwp_asan)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::gwp_asan)
    target_link_libraries (clickhouse_new_delete PRIVATE ch_contrib::gwp_asan)
endif()

# Otherwise it will slow down stack traces printing too much.
if ("${CMAKE_BUILD_TYPE_UC}" STREQUAL "DEBUG")
    set_source_files_properties(
            Common/Elf.cpp
            Common/Dwarf.cpp
            Common/SymbolIndex.cpp
            Common/ThreadFuzzer.cpp
            PROPERTIES COMPILE_FLAGS "-O2 ${WITHOUT_COVERAGE_FLAGS}")
endif()

target_link_libraries (clickhouse_common_io
        PRIVATE
            widechar_width
            ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
            common
            ch_contrib::double_conversion
            ch_contrib::dragonbox_to_chars
            ch_contrib::libdivide
)


target_link_libraries (clickhouse_compression
        PUBLIC
            pcg_random
            clickhouse_parsers
        PRIVATE
            ch_contrib::lz4
            ch_contrib::roaring
)

# Use X86 AVX2/AVX512 instructions to accelerate filter operations
set_source_files_properties(
        Columns/ColumnFixedString.cpp
        Columns/ColumnsCommon.cpp
        Columns/ColumnVector.cpp
        Columns/ColumnDecimal.cpp
        Columns/ColumnString.cpp
        PROPERTIES COMPILE_FLAGS "${X86_INTRINSICS_FLAGS}")

if (ENABLE_QPL)
    set_source_files_properties(
            Compression/CompressionCodecDeflateQpl.cpp
            PROPERTIES COMPILE_FLAGS "-mwaitpkg")
endif ()

target_link_libraries(clickhouse_common_io
        PUBLIC
            boost::program_options
            boost::system
            boost::context
            ch_contrib::cityhash
            ch_contrib::re2
            ch_contrib::zlib
            pcg_random
            Poco::Foundation
)

if (TARGET ch_contrib::fiu)
    target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::fiu)
endif()

if (TARGET ch_contrib::cpuid)
    target_link_libraries(clickhouse_common_io PRIVATE ch_contrib::cpuid)
endif()

if (TARGET ch_contrib::crc32-vpmsum)
    target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::crc32-vpmsum)
 endif()

if (TARGET ch_contrib::ssh)
    target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::ssh)
endif()

dbms_target_link_libraries(PUBLIC ch_contrib::abseil_swiss_tables)
target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::abseil_swiss_tables)

# Make dbms depend on roaring instead of clickhouse_common_io so that roaring itself can depend on clickhouse_common_io
# That way we we can redirect malloc/free functions avoiding circular dependencies
dbms_target_link_libraries(PUBLIC ch_contrib::roaring)

if (TARGET ch_contrib::rdkafka)
    dbms_target_link_libraries(PUBLIC ch_contrib::rdkafka ch_contrib::cppkafka)
endif()

if (TARGET ch_contrib::nats_io)
    dbms_target_link_libraries(PRIVATE ch_contrib::nats_io ch_contrib::uv)
endif()

if (TARGET ch_contrib::sasl2)
    dbms_target_link_libraries(PRIVATE ch_contrib::sasl2)
endif()

if (TARGET ch_contrib::krb5)
    dbms_target_link_libraries(PRIVATE ch_contrib::krb5)
    target_link_libraries (clickhouse_compression PRIVATE ch_contrib::krb5)
endif()

if (TARGET ch_contrib::nuraft)
    dbms_target_link_libraries(PUBLIC ch_contrib::nuraft)
    target_link_libraries (clickhouse_compression PUBLIC ch_contrib::nuraft)
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
    PUBLIC
        boost::system
        clickhouse_common_io
        Poco::MongoDB
        Poco::Redis
)

if (TARGET ch::mysqlxx)
    dbms_target_link_libraries (PUBLIC ch::mysqlxx)
endif()

dbms_target_link_libraries (
    PUBLIC
        boost::circular_buffer
        boost::heap)

target_link_libraries(clickhouse_common_io PUBLIC
    ch_contrib::miniselect
    ch_contrib::pdqsort)
dbms_target_link_libraries(PUBLIC
    ch_contrib::miniselect
    ch_contrib::pdqsort)

dbms_target_link_libraries(PRIVATE ch_contrib::zstd)
target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::zstd)
target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::xz)

if (TARGET ch_contrib::pocketfft)
    target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::pocketfft)
endif ()

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

set_source_files_properties(Common/CaresPTRResolver.cpp PROPERTIES COMPILE_FLAGS -Wno-reserved-identifier)
target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::c-ares)

if (TARGET OpenSSL::Crypto)
    dbms_target_link_libraries (PRIVATE OpenSSL::Crypto)
    target_link_libraries (clickhouse_common_io PRIVATE OpenSSL::Crypto)
endif ()

if (TARGET ch_contrib::ldap)
    dbms_target_link_libraries (PRIVATE ch_contrib::ldap ch_contrib::lber)
    target_link_libraries (clickhouse_compression PRIVATE ch_contrib::ldap ch_contrib::lber)
endif ()
dbms_target_link_libraries (PUBLIC ch_contrib::sparsehash)

if (TARGET ch_contrib::protobuf)
    dbms_target_link_libraries (PRIVATE ch_contrib::protobuf)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::protobuf)
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
    dbms_target_link_libraries (PRIVATE ch_contrib::azure_sdk)
endif()

if (TARGET ch_contrib::s2)
    dbms_target_link_libraries (PUBLIC ch_contrib::s2)
endif()

if (TARGET ch_contrib::vectorscan)
    dbms_target_link_libraries (PRIVATE ch_contrib::vectorscan)
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

if (TARGET ch_contrib::bcrypt)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::bcrypt)
endif()

if (TARGET ch_contrib::msgpack)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::msgpack)
endif()

if (TARGET ch_contrib::liburing)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::liburing)
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${LIBURING_COMPAT_INCLUDE_DIR} ${LIBURING_INCLUDE_DIR})
endif()

target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::fast_float)

if (USE_ORC)
    dbms_target_link_libraries(PUBLIC ${ORC_LIBRARIES})
    dbms_target_include_directories(SYSTEM BEFORE PUBLIC ${ORC_INCLUDE_DIR} "${PROJECT_BINARY_DIR}/contrib/orc/c++/include")
endif ()

if (TARGET ch_contrib::rocksdb)
    dbms_target_link_libraries(PUBLIC ch_contrib::rocksdb)
endif()

if (TARGET ch_contrib::libpqxx)
    dbms_target_link_libraries(PUBLIC ch_contrib::libpqxx)
endif()

if (TARGET ch_contrib::datasketches)
    dbms_target_link_libraries(PUBLIC ch_contrib::datasketches)
endif ()

target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::lz4)

if (TARGET ch_contrib::qpl)
    dbms_target_link_libraries(PUBLIC ch_contrib::qpl)
endif ()

if (TARGET ch_contrib::accel-config)
    dbms_target_link_libraries(PUBLIC ch_contrib::accel-config)
endif ()

if (TARGET ch_contrib::qatzstd_plugin)
    dbms_target_link_libraries(PUBLIC ch_contrib::qatzstd_plugin)
    target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::qatzstd_plugin)
endif ()

target_link_libraries(clickhouse_common_io PUBLIC boost::context)
dbms_target_link_libraries(PUBLIC boost::context)

if (ENABLE_NLP)
    dbms_target_link_libraries (PUBLIC ch_contrib::stemmer)
    dbms_target_link_libraries (PUBLIC ch_contrib::wnb)
    dbms_target_link_libraries (PUBLIC ch_contrib::lemmagen)
endif()

if (TARGET ch_contrib::ulid)
    dbms_target_link_libraries (PUBLIC ch_contrib::ulid)
endif()

if (TARGET ch_contrib::bzip2)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::bzip2)
endif()

if (TARGET ch_contrib::libarchive)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::libarchive)
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

if (TARGET ch_contrib::annoy)
    dbms_target_link_libraries(PUBLIC ch_contrib::annoy)
endif()

if (TARGET ch_contrib::usearch)
    dbms_target_link_libraries(PUBLIC ch_contrib::usearch)
endif()

if (TARGET ch_contrib::prometheus_protobufs)
    dbms_target_link_libraries (PUBLIC ch_contrib::prometheus_protobufs)
endif()

if (TARGET ch_rust::skim)
    dbms_target_include_directories(PRIVATE $<TARGET_PROPERTY:ch_rust::skim,INTERFACE_INCLUDE_DIRECTORIES>)
    dbms_target_link_libraries(PUBLIC ch_rust::skim)
endif()

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
        -Wno-sign-compare
    )

    target_link_libraries(unit_tests_dbms PRIVATE
        ch_contrib::gmock_all
        ch_contrib::gtest
        clickhouse_functions
        clickhouse_aggregate_functions
        clickhouse_parsers
        clickhouse_storages_system
        dbms
        clickhouse_common_config
        clickhouse_common_zookeeper
        hilite_comparator)

    if (TARGET ch_contrib::simdjson)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::simdjson)
    endif()

    if (TARGET ch_contrib::yaml_cpp)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::yaml_cpp)
    endif()

    if (TARGET ch_contrib::azure_sdk)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::azure_sdk)
    endif()

    if (TARGET ch_contrib::parquet)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::parquet)
    endif()
endif ()
