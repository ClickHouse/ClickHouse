set (CMAKE_CXX_STANDARD 17)

# === thrift

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp")
# contrib/thrift/lib/cpp/CMakeLists.txt
set(thriftcpp_SOURCES
        "${LIBRARY_DIR}/src/thrift/TApplicationException.cpp"
        "${LIBRARY_DIR}/src/thrift/TOutput.cpp"
        "${LIBRARY_DIR}/src/thrift/async/TAsyncChannel.cpp"
        "${LIBRARY_DIR}/src/thrift/async/TAsyncProtocolProcessor.cpp"
        "${LIBRARY_DIR}/src/thrift/async/TConcurrentClientSyncInfo.h"
        "${LIBRARY_DIR}/src/thrift/async/TConcurrentClientSyncInfo.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/ThreadManager.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/TimerManager.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/Util.cpp"
        "${LIBRARY_DIR}/src/thrift/processor/PeekProcessor.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TBase64Utils.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TDebugProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TJSONProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TMultiplexedProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/protocol/TProtocol.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TTransportException.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TFDTransport.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TSimpleFileTransport.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/THttpTransport.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/THttpClient.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/THttpServer.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TSocket.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TSocketPool.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TServerSocket.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TTransportUtils.cpp"
        "${LIBRARY_DIR}/src/thrift/transport/TBufferTransports.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TConnectedClient.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TServerFramework.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TSimpleServer.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TThreadPoolServer.cpp"
        "${LIBRARY_DIR}/src/thrift/server/TThreadedServer.cpp"
        )
set(thriftcpp_threads_SOURCES
        "${LIBRARY_DIR}/src/thrift/concurrency/ThreadFactory.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/Thread.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/Monitor.cpp"
        "${LIBRARY_DIR}/src/thrift/concurrency/Mutex.cpp"
        )
add_library(${THRIFT_LIBRARY} ${thriftcpp_SOURCES} ${thriftcpp_threads_SOURCES})
set_target_properties(${THRIFT_LIBRARY} PROPERTIES CXX_STANDARD 14) # REMOVE after https://github.com/apache/thrift/pull/1641
target_include_directories(${THRIFT_LIBRARY} SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src")
target_link_libraries (${THRIFT_LIBRARY} PRIVATE boost::headers_only)


# === orc

set(ORC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/orc/c++")
set(ORC_INCLUDE_DIR "${ORC_SOURCE_DIR}/include")
set(ORC_SOURCE_SRC_DIR "${ORC_SOURCE_DIR}/src")
# set(ORC_SOURCE_WRAP_DIR "${ORC_SOURCE_DIR}/wrap")

set(ORC_BUILD_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}/../orc/c++/src")
set(ORC_BUILD_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/../orc/c++/include")

set(GOOGLE_PROTOBUF_DIR "${Protobuf_INCLUDE_DIR}/")
set(ORC_ADDITION_SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(ARROW_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")

set(PROTOBUF_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
set(PROTO_DIR "${ORC_SOURCE_DIR}/../proto")


add_custom_command(OUTPUT orc_proto.pb.h orc_proto.pb.cc
        COMMAND ${PROTOBUF_EXECUTABLE}
        -I ${PROTO_DIR}
        --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
        "${PROTO_DIR}/orc_proto.proto")


# === flatbuffers
set(FLATBUFFERS_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/flatbuffers")
set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_SRC_DIR}/include")

# set flatbuffers CMake options
if (MAKE_STATIC_LIBRARIES)
    set(FLATBUFFERS_BUILD_FLATLIB ON CACHE BOOL "Enable the build of the flatbuffers library")
    set(FLATBUFFERS_BUILD_SHAREDLIB OFF CACHE BOOL "Disable the build of the flatbuffers shared library")
else ()
    set(FLATBUFFERS_BUILD_SHAREDLIB ON CACHE BOOL "Enable the build of the flatbuffers shared library")
    set(FLATBUFFERS_BUILD_FLATLIB OFF CACHE BOOL "Disable the build of the flatbuffers library")
endif ()
set(FLATBUFFERS_BUILD_TESTS OFF CACHE BOOL "Skip flatbuffers tests")

add_subdirectory(${FLATBUFFERS_SRC_DIR} "${FLATBUFFERS_BINARY_DIR}")

message(STATUS "FLATBUFFERS_LIBRARY: ${FLATBUFFERS_LIBRARY}")

# arrow-cmake cmake file calling orc cmake subroutine which detects certain compiler features.
# Apple Clang compiler failed to compile this code without specifying c++11 standard.
# As result these compiler features detected as absent. In result it failed to compile orc itself.
# In orc makefile there is code that sets flags, but arrow-cmake ignores these flags.
if (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    set(CXX11_FLAGS "-std=c++0x")
endif ()

set (ORC_CXX_HAS_INITIALIZER_LIST 1)
set (ORC_CXX_HAS_NOEXCEPT 1)
set (ORC_CXX_HAS_NULLPTR 1)
set (ORC_CXX_HAS_OVERRIDE 1)
set (ORC_CXX_HAS_UNIQUE_PTR 1)
set (ORC_CXX_HAS_CSTDINT 1)
set (ORC_CXX_HAS_THREAD_LOCAL 1)

include(orc_check.cmake)
configure_file("${ORC_INCLUDE_DIR}/orc/orc-config.hh.in" "${ORC_BUILD_INCLUDE_DIR}/orc/orc-config.hh")
configure_file("${ORC_SOURCE_SRC_DIR}/Adaptor.hh.in" "${ORC_BUILD_INCLUDE_DIR}/Adaptor.hh")


set(ORC_SRCS
        "${ARROW_SRC_DIR}/arrow/adapters/orc/adapter.cc"
        "${ARROW_SRC_DIR}/arrow/adapters/orc/adapter_util.cc"
        "${ORC_SOURCE_SRC_DIR}/Exceptions.cc"
        "${ORC_SOURCE_SRC_DIR}/OrcFile.cc"
        "${ORC_SOURCE_SRC_DIR}/Reader.cc"
        "${ORC_SOURCE_SRC_DIR}/ByteRLE.cc"
        "${ORC_SOURCE_SRC_DIR}/ColumnPrinter.cc"
        "${ORC_SOURCE_SRC_DIR}/ColumnReader.cc"
        "${ORC_SOURCE_SRC_DIR}/ColumnWriter.cc"
        "${ORC_SOURCE_SRC_DIR}/Common.cc"
        "${ORC_SOURCE_SRC_DIR}/Compression.cc"
        "${ORC_SOURCE_SRC_DIR}/Int128.cc"
        "${ORC_SOURCE_SRC_DIR}/LzoDecompressor.cc"
        "${ORC_SOURCE_SRC_DIR}/MemoryPool.cc"
        "${ORC_SOURCE_SRC_DIR}/RLE.cc"
        "${ORC_SOURCE_SRC_DIR}/RLEv1.cc"
        "${ORC_SOURCE_SRC_DIR}/RLEv2.cc"
        "${ORC_SOURCE_SRC_DIR}/Statistics.cc"
        "${ORC_SOURCE_SRC_DIR}/StripeStream.cc"
        "${ORC_SOURCE_SRC_DIR}/Timezone.cc"
        "${ORC_SOURCE_SRC_DIR}/TypeImpl.cc"
        "${ORC_SOURCE_SRC_DIR}/Vector.cc"
        "${ORC_SOURCE_SRC_DIR}/Writer.cc"
        "${ORC_SOURCE_SRC_DIR}/io/InputStream.cc"
        "${ORC_SOURCE_SRC_DIR}/io/OutputStream.cc"
        "${ORC_ADDITION_SOURCE_DIR}/orc_proto.pb.cc"
        )


# === arrow

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow")

configure_file("${LIBRARY_DIR}/util/config.h.cmake" "${CMAKE_CURRENT_BINARY_DIR}/cpp/src/arrow/util/config.h")

# arrow/cpp/src/arrow/CMakeLists.txt
set(ARROW_SRCS
        "${LIBRARY_DIR}/buffer.cc"
        "${LIBRARY_DIR}/builder.cc"
        "${LIBRARY_DIR}/chunked_array.cc"
        "${LIBRARY_DIR}/compare.cc"
        "${LIBRARY_DIR}/datum.cc"
        "${LIBRARY_DIR}/device.cc"
        "${LIBRARY_DIR}/extension_type.cc"
        "${LIBRARY_DIR}/memory_pool.cc"
        "${LIBRARY_DIR}/pretty_print.cc"
        "${LIBRARY_DIR}/record_batch.cc"
        "${LIBRARY_DIR}/result.cc"
        "${LIBRARY_DIR}/scalar.cc"
        "${LIBRARY_DIR}/sparse_tensor.cc"
        "${LIBRARY_DIR}/status.cc"
        "${LIBRARY_DIR}/table_builder.cc"
        "${LIBRARY_DIR}/table.cc"
        "${LIBRARY_DIR}/tensor.cc"
        "${LIBRARY_DIR}/type.cc"
        "${LIBRARY_DIR}/visitor.cc"

        "${LIBRARY_DIR}/array/array_base.cc"
        "${LIBRARY_DIR}/array/array_binary.cc"
        "${LIBRARY_DIR}/array/array_decimal.cc"
        "${LIBRARY_DIR}/array/array_dict.cc"
        "${LIBRARY_DIR}/array/array_nested.cc"
        "${LIBRARY_DIR}/array/array_primitive.cc"
        "${LIBRARY_DIR}/array/builder_adaptive.cc"
        "${LIBRARY_DIR}/array/builder_base.cc"
        "${LIBRARY_DIR}/array/builder_binary.cc"
        "${LIBRARY_DIR}/array/builder_decimal.cc"
        "${LIBRARY_DIR}/array/builder_dict.cc"
        "${LIBRARY_DIR}/array/builder_nested.cc"
        "${LIBRARY_DIR}/array/builder_primitive.cc"
        "${LIBRARY_DIR}/array/builder_union.cc"
        "${LIBRARY_DIR}/array/concatenate.cc"
        "${LIBRARY_DIR}/array/data.cc"
        "${LIBRARY_DIR}/array/diff.cc"
        "${LIBRARY_DIR}/array/util.cc"
        "${LIBRARY_DIR}/array/validate.cc"

        "${LIBRARY_DIR}/compute/api_aggregate.cc"
        "${LIBRARY_DIR}/compute/api_scalar.cc"
        "${LIBRARY_DIR}/compute/api_vector.cc"
        "${LIBRARY_DIR}/compute/cast.cc"
        "${LIBRARY_DIR}/compute/exec.cc"
        "${LIBRARY_DIR}/compute/function.cc"
        "${LIBRARY_DIR}/compute/function_internal.cc"
        "${LIBRARY_DIR}/compute/kernel.cc"
        "${LIBRARY_DIR}/compute/registry.cc"

        "${LIBRARY_DIR}/compute/exec/exec_plan.cc"
        "${LIBRARY_DIR}/compute/exec/expression.cc"
        "${LIBRARY_DIR}/compute/exec/key_compare.cc"
        "${LIBRARY_DIR}/compute/exec/key_encode.cc"
        "${LIBRARY_DIR}/compute/exec/key_hash.cc"
        "${LIBRARY_DIR}/compute/exec/key_map.cc"
        "${LIBRARY_DIR}/compute/exec/util.cc"

        "${LIBRARY_DIR}/compute/kernels/aggregate_basic.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_mode.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_quantile.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_tdigest.cc"
        "${LIBRARY_DIR}/compute/kernels/aggregate_var_std.cc"
        "${LIBRARY_DIR}/compute/kernels/codegen_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/hash_aggregate.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_arithmetic.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_boolean.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_boolean.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_dictionary.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_nested.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_numeric.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_string.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_cast_temporal.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_compare.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_fill_null.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_if_else.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_nested.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_set_lookup.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_string.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_temporal.cc"
        "${LIBRARY_DIR}/compute/kernels/scalar_validity.cc"
        "${LIBRARY_DIR}/compute/kernels/util_internal.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_hash.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_nested.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_replace.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_selection.cc"
        "${LIBRARY_DIR}/compute/kernels/vector_sort.cc"

        "${LIBRARY_DIR}/csv/chunker.cc"
        "${LIBRARY_DIR}/csv/column_builder.cc"
        "${LIBRARY_DIR}/csv/column_decoder.cc"
        "${LIBRARY_DIR}/csv/converter.cc"
        "${LIBRARY_DIR}/csv/options.cc"
        "${LIBRARY_DIR}/csv/parser.cc"
        "${LIBRARY_DIR}/csv/reader.cc"
        "${LIBRARY_DIR}/csv/writer.cc"

        "${LIBRARY_DIR}/ipc/dictionary.cc"
        "${LIBRARY_DIR}/ipc/feather.cc"
        "${LIBRARY_DIR}/ipc/message.cc"
        "${LIBRARY_DIR}/ipc/metadata_internal.cc"
        "${LIBRARY_DIR}/ipc/options.cc"
        "${LIBRARY_DIR}/ipc/reader.cc"
        "${LIBRARY_DIR}/ipc/writer.cc"

        "${LIBRARY_DIR}/io/buffered.cc"
        "${LIBRARY_DIR}/io/caching.cc"
        "${LIBRARY_DIR}/io/compressed.cc"
        "${LIBRARY_DIR}/io/file.cc"
        "${LIBRARY_DIR}/io/interfaces.cc"
        "${LIBRARY_DIR}/io/memory.cc"
        "${LIBRARY_DIR}/io/slow.cc"
        "${LIBRARY_DIR}/io/stdio.cc"
        "${LIBRARY_DIR}/io/transform.cc"

        "${LIBRARY_DIR}/tensor/coo_converter.cc"
        "${LIBRARY_DIR}/tensor/csf_converter.cc"
        "${LIBRARY_DIR}/tensor/csx_converter.cc"

        "${LIBRARY_DIR}/util/basic_decimal.cc"
        "${LIBRARY_DIR}/util/bit_block_counter.cc"
        "${LIBRARY_DIR}/util/bit_run_reader.cc"
        "${LIBRARY_DIR}/util/bit_util.cc"
        "${LIBRARY_DIR}/util/bitmap_builders.cc"
        "${LIBRARY_DIR}/util/bitmap_ops.cc"
        "${LIBRARY_DIR}/util/bitmap.cc"
        "${LIBRARY_DIR}/util/bpacking.cc"
        "${LIBRARY_DIR}/util/cancel.cc"
        "${LIBRARY_DIR}/util/compression.cc"
        "${LIBRARY_DIR}/util/cpu_info.cc"
        "${LIBRARY_DIR}/util/decimal.cc"
        "${LIBRARY_DIR}/util/delimiting.cc"
        "${LIBRARY_DIR}/util/formatting.cc"
        "${LIBRARY_DIR}/util/future.cc"
        "${LIBRARY_DIR}/util/int_util.cc"
        "${LIBRARY_DIR}/util/io_util.cc"
        "${LIBRARY_DIR}/util/key_value_metadata.cc"
        "${LIBRARY_DIR}/util/logging.cc"
        "${LIBRARY_DIR}/util/memory.cc"
        "${LIBRARY_DIR}/util/mutex.cc"
        "${LIBRARY_DIR}/util/string_builder.cc"
        "${LIBRARY_DIR}/util/string.cc"
        "${LIBRARY_DIR}/util/task_group.cc"
        "${LIBRARY_DIR}/util/tdigest.cc"
        "${LIBRARY_DIR}/util/thread_pool.cc"
        "${LIBRARY_DIR}/util/time.cc"
        "${LIBRARY_DIR}/util/trie.cc"
        "${LIBRARY_DIR}/util/utf8.cc"
        "${LIBRARY_DIR}/util/value_parsing.cc"

        "${LIBRARY_DIR}/vendored/base64.cpp"
        ${ORC_SRCS}
        )

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIBRARY)
    set(ARROW_WITH_SNAPPY 1)
endif ()

if (ZLIB_INCLUDE_DIR AND ZLIB_LIBRARIES)
    set(ARROW_WITH_ZLIB 1)
endif ()

if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
    set(ARROW_WITH_ZSTD 1)
endif ()

add_definitions(-DARROW_WITH_LZ4)
SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_lz4.cc" ${ARROW_SRCS})

if (ARROW_WITH_SNAPPY)
    add_definitions(-DARROW_WITH_SNAPPY)
    SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_snappy.cc" ${ARROW_SRCS})
endif ()

if (ARROW_WITH_ZLIB)
    add_definitions(-DARROW_WITH_ZLIB)
    SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_zlib.cc" ${ARROW_SRCS})
endif ()

if (ARROW_WITH_ZSTD)
    add_definitions(-DARROW_WITH_ZSTD)
    SET(ARROW_SRCS "${LIBRARY_DIR}/util/compression_zstd.cc" ${ARROW_SRCS})
endif ()


add_library(${ARROW_LIBRARY} ${ARROW_SRCS})

# Arrow dependencies
add_dependencies(${ARROW_LIBRARY} ${FLATBUFFERS_LIBRARY})

target_link_libraries(${ARROW_LIBRARY} PRIVATE ${FLATBUFFERS_LIBRARY} boost::filesystem)

if (USE_INTERNAL_PROTOBUF_LIBRARY)
    add_dependencies(${ARROW_LIBRARY} protoc)
endif ()

target_include_directories(${ARROW_LIBRARY} SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")
target_include_directories(${ARROW_LIBRARY} SYSTEM PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/cpp/src")
target_link_libraries(${ARROW_LIBRARY} PRIVATE ${DOUBLE_CONVERSION_LIBRARIES} ${Protobuf_LIBRARY})
target_link_libraries(${ARROW_LIBRARY} PRIVATE lz4)
if (ARROW_WITH_SNAPPY)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${SNAPPY_LIBRARY})
endif ()
if (ARROW_WITH_ZLIB)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${ZLIB_LIBRARIES})
endif ()
if (ARROW_WITH_ZSTD)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${ZSTD_LIBRARY})
    target_include_directories(${ARROW_LIBRARY} SYSTEM BEFORE PRIVATE ${ZLIB_INCLUDE_DIR})
endif ()

target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ORC_INCLUDE_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ORC_SOURCE_SRC_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ORC_SOURCE_WRAP_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${GOOGLE_PROTOBUF_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ORC_BUILD_SRC_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ORC_BUILD_INCLUDE_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ORC_ADDITION_SOURCE_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ARROW_SRC_DIR})
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${FLATBUFFERS_INCLUDE_DIR})

# === parquet

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/parquet")
set(GEN_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/generated")
# arrow/cpp/src/parquet/CMakeLists.txt
set(PARQUET_SRCS
        "${LIBRARY_DIR}/arrow/path_internal.cc"
        "${LIBRARY_DIR}/arrow/reader.cc"
        "${LIBRARY_DIR}/arrow/reader_internal.cc"
        "${LIBRARY_DIR}/arrow/schema.cc"
        "${LIBRARY_DIR}/arrow/schema_internal.cc"
        "${LIBRARY_DIR}/arrow/writer.cc"
        "${LIBRARY_DIR}/bloom_filter.cc"
        "${LIBRARY_DIR}/column_reader.cc"
        "${LIBRARY_DIR}/column_scanner.cc"
        "${LIBRARY_DIR}/column_writer.cc"
        "${LIBRARY_DIR}/encoding.cc"
        "${LIBRARY_DIR}/encryption/encryption.cc"
        "${LIBRARY_DIR}/encryption/encryption_internal.cc"
        "${LIBRARY_DIR}/encryption/internal_file_decryptor.cc"
        "${LIBRARY_DIR}/encryption/internal_file_encryptor.cc"
        "${LIBRARY_DIR}/exception.cc"
        "${LIBRARY_DIR}/file_reader.cc"
        "${LIBRARY_DIR}/file_writer.cc"
        "${LIBRARY_DIR}/level_conversion.cc"
        "${LIBRARY_DIR}/level_comparison.cc"
        "${LIBRARY_DIR}/metadata.cc"
        "${LIBRARY_DIR}/murmur3.cc"
        "${LIBRARY_DIR}/platform.cc"
        "${LIBRARY_DIR}/printer.cc"
        "${LIBRARY_DIR}/properties.cc"
        "${LIBRARY_DIR}/schema.cc"
        "${LIBRARY_DIR}/statistics.cc"
        "${LIBRARY_DIR}/stream_reader.cc"
        "${LIBRARY_DIR}/stream_writer.cc"
        "${LIBRARY_DIR}/types.cc"

        "${GEN_LIBRARY_DIR}/parquet_constants.cpp"
        "${GEN_LIBRARY_DIR}/parquet_types.cpp"
        )
#list(TRANSFORM PARQUET_SRCS PREPEND "${LIBRARY_DIR}/") # cmake 3.12
add_library(${PARQUET_LIBRARY} ${PARQUET_SRCS})
target_include_directories(${PARQUET_LIBRARY} SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src" "${CMAKE_CURRENT_SOURCE_DIR}/cpp/src" PRIVATE ${OPENSSL_INCLUDE_DIR})
include("${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/ConfigureChecks.cmake") # makes config.h
target_link_libraries(${PARQUET_LIBRARY} PUBLIC ${ARROW_LIBRARY} PRIVATE ${THRIFT_LIBRARY} boost::headers_only boost::regex ${OPENSSL_LIBRARIES})

if (SANITIZE STREQUAL "undefined")
    target_compile_options(${PARQUET_LIBRARY} PRIVATE -fno-sanitize=undefined)
    target_compile_options(${ARROW_LIBRARY} PRIVATE -fno-sanitize=undefined)
endif ()

# === tools

set(TOOLS_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/tools/parquet")
set(PARQUET_TOOLS parquet_dump_schema parquet_reader parquet_scan)
foreach (TOOL ${PARQUET_TOOLS})
    add_executable(${TOOL} "${TOOLS_DIR}/${TOOL}.cc")
    target_link_libraries(${TOOL} PRIVATE ${PARQUET_LIBRARY})
endforeach ()
