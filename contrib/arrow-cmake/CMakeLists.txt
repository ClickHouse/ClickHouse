include(ExternalProject)

set (CMAKE_CXX_STANDARD 17)

# === thrift

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp)
# contrib/thrift/lib/cpp/CMakeLists.txt
set(thriftcpp_SOURCES
        ${LIBRARY_DIR}/src/thrift/TApplicationException.cpp
        ${LIBRARY_DIR}/src/thrift/TOutput.cpp
        ${LIBRARY_DIR}/src/thrift/async/TAsyncChannel.cpp
        ${LIBRARY_DIR}/src/thrift/async/TAsyncProtocolProcessor.cpp
        ${LIBRARY_DIR}/src/thrift/async/TConcurrentClientSyncInfo.h
        ${LIBRARY_DIR}/src/thrift/async/TConcurrentClientSyncInfo.cpp
        ${LIBRARY_DIR}/src/thrift/concurrency/ThreadManager.cpp
        ${LIBRARY_DIR}/src/thrift/concurrency/TimerManager.cpp
        ${LIBRARY_DIR}/src/thrift/concurrency/Util.cpp
        ${LIBRARY_DIR}/src/thrift/processor/PeekProcessor.cpp
        ${LIBRARY_DIR}/src/thrift/protocol/TBase64Utils.cpp
        ${LIBRARY_DIR}/src/thrift/protocol/TDebugProtocol.cpp
        ${LIBRARY_DIR}/src/thrift/protocol/TJSONProtocol.cpp
        ${LIBRARY_DIR}/src/thrift/protocol/TMultiplexedProtocol.cpp
        ${LIBRARY_DIR}/src/thrift/protocol/TProtocol.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TTransportException.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TFDTransport.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TSimpleFileTransport.cpp
        ${LIBRARY_DIR}/src/thrift/transport/THttpTransport.cpp
        ${LIBRARY_DIR}/src/thrift/transport/THttpClient.cpp
        ${LIBRARY_DIR}/src/thrift/transport/THttpServer.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TSocket.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TSocketPool.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TServerSocket.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TTransportUtils.cpp
        ${LIBRARY_DIR}/src/thrift/transport/TBufferTransports.cpp
        ${LIBRARY_DIR}/src/thrift/server/TConnectedClient.cpp
        ${LIBRARY_DIR}/src/thrift/server/TServerFramework.cpp
        ${LIBRARY_DIR}/src/thrift/server/TSimpleServer.cpp
        ${LIBRARY_DIR}/src/thrift/server/TThreadPoolServer.cpp
        ${LIBRARY_DIR}/src/thrift/server/TThreadedServer.cpp
        )
set(thriftcpp_threads_SOURCES
        ${LIBRARY_DIR}/src/thrift/concurrency/ThreadFactory.cpp
        ${LIBRARY_DIR}/src/thrift/concurrency/Thread.cpp
        ${LIBRARY_DIR}/src/thrift/concurrency/Monitor.cpp
        ${LIBRARY_DIR}/src/thrift/concurrency/Mutex.cpp
        )
add_library(${THRIFT_LIBRARY} ${thriftcpp_SOURCES} ${thriftcpp_threads_SOURCES})
set_target_properties(${THRIFT_LIBRARY} PROPERTIES CXX_STANDARD 14) # REMOVE after https://github.com/apache/thrift/pull/1641
target_include_directories(${THRIFT_LIBRARY} SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src PRIVATE ${Boost_INCLUDE_DIRS})


# === orc

set(ORC_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/orc/c++)
set(ORC_INCLUDE_DIR ${ORC_SOURCE_DIR}/include)
set(ORC_SOURCE_SRC_DIR ${ORC_SOURCE_DIR}/src)
set(ORC_SOURCE_WRAP_DIR ${ORC_SOURCE_DIR}/wrap)

set(ORC_BUILD_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/../orc/c++/src)
set(ORC_BUILD_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/../orc/c++/include)

set(GOOGLE_PROTOBUF_DIR ${Protobuf_INCLUDE_DIR}/)
set(ORC_ADDITION_SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(ARROW_SRC_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src)

set(PROTOBUF_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
set(PROTO_DIR ${ORC_SOURCE_DIR}/../proto)


add_custom_command(OUTPUT orc_proto.pb.h orc_proto.pb.cc
        COMMAND ${PROTOBUF_EXECUTABLE}
        -I ${PROTO_DIR}
        --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
        "${PROTO_DIR}/orc_proto.proto")


# === flatbuffers

##############################################################
# fbs - Step 1: build flatbuffers lib and flatc compiler
##############################################################
set(FLATBUFFERS_SRC_DIR ${ClickHouse_SOURCE_DIR}/contrib/flatbuffers)
set(FLATBUFFERS_BINARY_DIR ${ClickHouse_BINARY_DIR}/contrib/flatbuffers)
set(FLATBUFFERS_INCLUDE_DIR ${FLATBUFFERS_SRC_DIR}/include)
set(FLATBUFFERS_COMPILER "$<TARGET_FILE:flatc>")

# set flatbuffers CMake options
if (${USE_STATIC_LIBRARIES})
    set(FLATBUFFERS_BUILD_FLATLIB ON CACHE BOOL "Enable the build of the flatbuffers library")
    set(FLATBUFFERS_BUILD_SHAREDLIB OFF CACHE BOOL "Disable the build of the flatbuffers shared library")
else ()
    set(FLATBUFFERS_BUILD_SHAREDLIB ON CACHE BOOL "Enable the build of the flatbuffers shared library")
    set(FLATBUFFERS_BUILD_FLATLIB OFF CACHE BOOL "Disable the build of the flatbuffers library")
endif ()
set(FLATBUFFERS_BUILD_FLATC ON CACHE BOOL "Build flatbuffers compiler")
set(FLATBUFFERS_BUILD_TESTS OFF CACHE BOOL "Skip flatbuffers tests")

add_subdirectory(${FLATBUFFERS_SRC_DIR} "${FLATBUFFERS_BINARY_DIR}")

###################################
# fbs - Step 2: compile *.fbs files
###################################
set(ARROW_IPC_SRC_DIR ${ARROW_SRC_DIR}/arrow/ipc)
set(ARROW_FORMAT_SRC_DIR ${ARROW_SRC_DIR}/../../format)

set(ARROW_GENERATED_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/arrow_gen_headers)
set(FLATBUFFERS_COMPILED_OUT_DIR ${ARROW_GENERATED_INCLUDE_DIR}/arrow/ipc)

set(FBS_OUTPUT_FILES
        "${FLATBUFFERS_COMPILED_OUT_DIR}/File_generated.h"
        "${FLATBUFFERS_COMPILED_OUT_DIR}/Message_generated.h"
        "${FLATBUFFERS_COMPILED_OUT_DIR}/feather_generated.h"
        "${FLATBUFFERS_COMPILED_OUT_DIR}/Schema_generated.h"
        "${FLATBUFFERS_COMPILED_OUT_DIR}/SparseTensor_generated.h"
        "${FLATBUFFERS_COMPILED_OUT_DIR}/Tensor_generated.h")

set(FBS_SRC
        ${ARROW_FORMAT_SRC_DIR}/Message.fbs
        ${ARROW_FORMAT_SRC_DIR}/File.fbs
        ${ARROW_FORMAT_SRC_DIR}/Schema.fbs
        ${ARROW_FORMAT_SRC_DIR}/Tensor.fbs
        ${ARROW_FORMAT_SRC_DIR}/SparseTensor.fbs
        ${ARROW_IPC_SRC_DIR}/feather.fbs)

foreach (FIL ${FBS_SRC})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    list(APPEND ABS_FBS_SRC ${ABS_FIL})
endforeach ()

message(STATUS "FLATBUFFERS_LIBRARY: ${FLATBUFFERS_LIBRARY}, FLATBUFFERS_COMPILER: ${FLATBUFFERS_COMPILER}")
message(STATUS "FLATBUFFERS_COMPILED_OUT_DIR: ${FLATBUFFERS_COMPILED_OUT_DIR}")
message(STATUS "flatc: ${FLATBUFFERS_COMPILER} -c -o ${FLATBUFFERS_COMPILED_OUT_DIR}/ ${ABS_FBS_SRC}")

add_custom_command(OUTPUT ${FBS_OUTPUT_FILES}
        COMMAND ${FLATBUFFERS_COMPILER}
        -c
        -o
        ${FLATBUFFERS_COMPILED_OUT_DIR}/
        ${ABS_FBS_SRC}
        DEPENDS flatc ${ABS_FBS_SRC}
        COMMENT "Running flatc compiler on ${ABS_FBS_SRC}"
        VERBATIM)

add_custom_target(metadata_fbs DEPENDS ${FBS_OUTPUT_FILES})
add_dependencies(metadata_fbs flatc)

# arrow-cmake cmake file calling orc cmake subroutine which detects certain compiler features.
# Apple Clang compiler failed to compile this code without specifying c++11 standard. 
# As result these compiler features detected as absent. In result it failed to compile orc itself.
# In orc makefile there is code that sets flags, but arrow-cmake ignores these flags.
if (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    set(CXX11_FLAGS "-std=c++0x")
endif ()

include(${ClickHouse_SOURCE_DIR}/contrib/orc/cmake_modules/CheckSourceCompiles.cmake)
include(orc_check.cmake)
configure_file("${ORC_INCLUDE_DIR}/orc/orc-config.hh.in" "${ORC_BUILD_INCLUDE_DIR}/orc/orc-config.hh")
configure_file("${ORC_SOURCE_SRC_DIR}/Adaptor.hh.in" "${ORC_BUILD_INCLUDE_DIR}/Adaptor.hh")


set(ORC_SRCS
        ${ARROW_SRC_DIR}/arrow/adapters/orc/adapter.cc
        ${ARROW_SRC_DIR}/arrow/adapters/orc/adapter_util.cc
        ${ORC_SOURCE_SRC_DIR}/Exceptions.cc
        ${ORC_SOURCE_SRC_DIR}/OrcFile.cc
        ${ORC_SOURCE_SRC_DIR}/Reader.cc
        ${ORC_SOURCE_SRC_DIR}/ByteRLE.cc
        ${ORC_SOURCE_SRC_DIR}/ColumnPrinter.cc
        ${ORC_SOURCE_SRC_DIR}/ColumnReader.cc
        ${ORC_SOURCE_SRC_DIR}/ColumnWriter.cc
        ${ORC_SOURCE_SRC_DIR}/Common.cc
        ${ORC_SOURCE_SRC_DIR}/Compression.cc
        ${ORC_SOURCE_SRC_DIR}/Exceptions.cc
        ${ORC_SOURCE_SRC_DIR}/Int128.cc
        ${ORC_SOURCE_SRC_DIR}/LzoDecompressor.cc
        ${ORC_SOURCE_SRC_DIR}/MemoryPool.cc
        ${ORC_SOURCE_SRC_DIR}/OrcFile.cc
        ${ORC_SOURCE_SRC_DIR}/Reader.cc
        ${ORC_SOURCE_SRC_DIR}/RLE.cc
        ${ORC_SOURCE_SRC_DIR}/RLEv1.cc
        ${ORC_SOURCE_SRC_DIR}/RLEv2.cc
        ${ORC_SOURCE_SRC_DIR}/Statistics.cc
        ${ORC_SOURCE_SRC_DIR}/StripeStream.cc
        ${ORC_SOURCE_SRC_DIR}/Timezone.cc
        ${ORC_SOURCE_SRC_DIR}/TypeImpl.cc
        ${ORC_SOURCE_SRC_DIR}/Vector.cc
        ${ORC_SOURCE_SRC_DIR}/Writer.cc
        ${ORC_SOURCE_SRC_DIR}/io/InputStream.cc
        ${ORC_SOURCE_SRC_DIR}/io/OutputStream.cc
        ${ORC_ADDITION_SOURCE_DIR}/orc_proto.pb.cc
        )


# === arrow

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow)

configure_file("${LIBRARY_DIR}/util/config.h.cmake" "${CMAKE_CURRENT_SOURCE_DIR}/cpp/src/arrow/util/config.h")

# arrow/cpp/src/arrow/CMakeLists.txt
set(ARROW_SRCS
        ${LIBRARY_DIR}/array.cc
        ${LIBRARY_DIR}/buffer.cc
        ${LIBRARY_DIR}/builder.cc
        ${LIBRARY_DIR}/compare.cc
        ${LIBRARY_DIR}/extension_type.cc
        ${LIBRARY_DIR}/memory_pool.cc
        ${LIBRARY_DIR}/pretty_print.cc
        ${LIBRARY_DIR}/record_batch.cc
        ${LIBRARY_DIR}/result.cc
        ${LIBRARY_DIR}/scalar.cc
        ${LIBRARY_DIR}/sparse_tensor.cc
        ${LIBRARY_DIR}/status.cc
        ${LIBRARY_DIR}/table_builder.cc
        ${LIBRARY_DIR}/table.cc
        ${LIBRARY_DIR}/tensor.cc
        ${LIBRARY_DIR}/type.cc
        ${LIBRARY_DIR}/visitor.cc

        ${LIBRARY_DIR}/array/builder_adaptive.cc
        ${LIBRARY_DIR}/array/builder_base.cc
        ${LIBRARY_DIR}/array/builder_binary.cc
        ${LIBRARY_DIR}/array/builder_decimal.cc
        ${LIBRARY_DIR}/array/builder_dict.cc
        ${LIBRARY_DIR}/array/builder_nested.cc
        ${LIBRARY_DIR}/array/builder_primitive.cc
        ${LIBRARY_DIR}/array/builder_union.cc
        ${LIBRARY_DIR}/array/concatenate.cc
        ${LIBRARY_DIR}/array/dict_internal.cc
        ${LIBRARY_DIR}/array/diff.cc

        ${LIBRARY_DIR}/csv/converter.cc
        ${LIBRARY_DIR}/csv/chunker.cc
        ${LIBRARY_DIR}/csv/column_builder.cc
        ${LIBRARY_DIR}/csv/options.cc
        ${LIBRARY_DIR}/csv/parser.cc
        ${LIBRARY_DIR}/csv/reader.cc

        ${LIBRARY_DIR}/ipc/dictionary.cc
        ${LIBRARY_DIR}/ipc/feather.cc
        ${LIBRARY_DIR}/ipc/message.cc
        ${LIBRARY_DIR}/ipc/metadata_internal.cc
        ${LIBRARY_DIR}/ipc/options.cc
        ${LIBRARY_DIR}/ipc/reader.cc
        ${LIBRARY_DIR}/ipc/writer.cc

        ${LIBRARY_DIR}/io/buffered.cc
        ${LIBRARY_DIR}/io/compressed.cc
        ${LIBRARY_DIR}/io/file.cc
        ${LIBRARY_DIR}/io/interfaces.cc
        ${LIBRARY_DIR}/io/memory.cc
        ${LIBRARY_DIR}/io/readahead.cc
        ${LIBRARY_DIR}/io/slow.cc

        ${LIBRARY_DIR}/util/basic_decimal.cc
        ${LIBRARY_DIR}/util/bit_util.cc
        ${LIBRARY_DIR}/util/compression.cc
        ${LIBRARY_DIR}/util/compression_lz4.cc
        ${LIBRARY_DIR}/util/compression_snappy.cc
        ${LIBRARY_DIR}/util/compression_zlib.cc
        ${LIBRARY_DIR}/util/compression_zstd.cc
        ${LIBRARY_DIR}/util/cpu_info.cc
        ${LIBRARY_DIR}/util/decimal.cc
        ${LIBRARY_DIR}/util/int_util.cc
        ${LIBRARY_DIR}/util/io_util.cc
        ${LIBRARY_DIR}/util/key_value_metadata.cc
        ${LIBRARY_DIR}/util/logging.cc
        ${LIBRARY_DIR}/util/memory.cc
        ${LIBRARY_DIR}/util/string_builder.cc
        ${LIBRARY_DIR}/util/string.cc
        ${LIBRARY_DIR}/util/task_group.cc
        ${LIBRARY_DIR}/util/thread_pool.cc
        ${LIBRARY_DIR}/util/trie.cc
        ${LIBRARY_DIR}/util/utf8.cc

        ${LIBRARY_DIR}/vendored/base64.cpp
        ${ORC_SRCS}
        )

set(ARROW_SRCS ${ARROW_SRCS}
        ${LIBRARY_DIR}/compute/context.cc
        ${LIBRARY_DIR}/compute/kernels/boolean.cc
        ${LIBRARY_DIR}/compute/kernels/cast.cc
        ${LIBRARY_DIR}/compute/kernels/hash.cc
        ${LIBRARY_DIR}/compute/kernels/util_internal.cc
        )

if (LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
    set(ARROW_WITH_LZ4 1)
endif ()

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIBRARY)
    set(ARROW_WITH_SNAPPY 1)
endif ()

if (ZLIB_INCLUDE_DIR AND ZLIB_LIBRARIES)
    set(ARROW_WITH_ZLIB 1)
endif ()

if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
    set(ARROW_WITH_ZSTD 1)
endif ()

if (ARROW_WITH_LZ4)
    add_definitions(-DARROW_WITH_LZ4)
    SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_lz4.cc ${ARROW_SRCS})
endif ()

if (ARROW_WITH_SNAPPY)
    add_definitions(-DARROW_WITH_SNAPPY)
    SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_snappy.cc ${ARROW_SRCS})
endif ()

if (ARROW_WITH_ZLIB)
    add_definitions(-DARROW_WITH_ZLIB)
    SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_zlib.cc ${ARROW_SRCS})
endif ()

if (ARROW_WITH_ZSTD)
    add_definitions(-DARROW_WITH_ZSTD)
    SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_zstd.cc ${ARROW_SRCS})
endif ()


add_library(${ARROW_LIBRARY} ${ARROW_SRCS})

# Arrow dependencies
add_dependencies(${ARROW_LIBRARY} ${FLATBUFFERS_LIBRARY} metadata_fbs)

target_link_libraries(${ARROW_LIBRARY} PRIVATE boost_system_internal boost_filesystem_internal boost_regex_internal)
target_link_libraries(${ARROW_LIBRARY} PRIVATE ${FLATBUFFERS_LIBRARY})

if (USE_INTERNAL_PROTOBUF_LIBRARY)
    add_dependencies(${ARROW_LIBRARY} protoc)
endif ()

target_include_directories(${ARROW_LIBRARY} SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src ${Boost_INCLUDE_DIRS})
target_link_libraries(${ARROW_LIBRARY} PRIVATE ${DOUBLE_CONVERSION_LIBRARIES} ${Protobuf_LIBRARY})
if (ARROW_WITH_LZ4)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${LZ4_LIBRARY})
endif ()
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
target_include_directories(${ARROW_LIBRARY} PRIVATE SYSTEM ${ARROW_GENERATED_INCLUDE_DIR})

# === parquet

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/parquet)
# arrow/cpp/src/parquet/CMakeLists.txt
set(PARQUET_SRCS
        ${LIBRARY_DIR}/arrow/reader.cc
        ${LIBRARY_DIR}/arrow/reader_internal.cc
        ${LIBRARY_DIR}/arrow/schema.cc
        ${LIBRARY_DIR}/arrow/writer.cc
        ${LIBRARY_DIR}/bloom_filter.cc
        ${LIBRARY_DIR}/column_reader.cc
        ${LIBRARY_DIR}/column_scanner.cc
        ${LIBRARY_DIR}/column_writer.cc
        ${LIBRARY_DIR}/deprecated_io.cc
        ${LIBRARY_DIR}/encoding.cc
        ${LIBRARY_DIR}/file_reader.cc
        ${LIBRARY_DIR}/file_writer.cc
        ${LIBRARY_DIR}/metadata.cc
        ${LIBRARY_DIR}/murmur3.cc
        ${LIBRARY_DIR}/platform.cc
        ${LIBRARY_DIR}/printer.cc
        ${LIBRARY_DIR}/properties.cc
        ${LIBRARY_DIR}/schema.cc
        ${LIBRARY_DIR}/statistics.cc
        ${LIBRARY_DIR}/types.cc
        )
#list(TRANSFORM PARQUET_SRCS PREPEND ${LIBRARY_DIR}/) # cmake 3.12
list(APPEND PARQUET_SRCS
        ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src/parquet/parquet_constants.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src/parquet/parquet_types.cpp
        )
add_library(${PARQUET_LIBRARY} ${PARQUET_SRCS})
target_include_directories(${PARQUET_LIBRARY} SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)
include(${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/ConfigureChecks.cmake) # makes config.h
target_link_libraries(${PARQUET_LIBRARY} PUBLIC ${ARROW_LIBRARY} PRIVATE ${THRIFT_LIBRARY} ${Boost_REGEX_LIBRARY})
target_include_directories(${PARQUET_LIBRARY} PRIVATE ${Boost_INCLUDE_DIRS})

if (SANITIZE STREQUAL "undefined")
    target_compile_options(${PARQUET_LIBRARY} PRIVATE -fno-sanitize=undefined)
    target_compile_options(${ARROW_LIBRARY} PRIVATE -fno-sanitize=undefined)
endif ()

# === tools

set(TOOLS_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/tools/parquet)
set(PARQUET_TOOLS parquet_dump_schema parquet_reader parquet_scan)
foreach (TOOL ${PARQUET_TOOLS})
    add_executable(${TOOL} ${TOOLS_DIR}/${TOOL}.cc)
    target_link_libraries(${TOOL} PRIVATE ${PARQUET_LIBRARY})
endforeach ()
