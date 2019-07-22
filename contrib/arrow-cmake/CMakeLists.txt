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
set( thriftcpp_threads_SOURCES
    ${LIBRARY_DIR}/src/thrift/concurrency/ThreadFactory.cpp
    ${LIBRARY_DIR}/src/thrift/concurrency/Thread.cpp
    ${LIBRARY_DIR}/src/thrift/concurrency/Monitor.cpp
    ${LIBRARY_DIR}/src/thrift/concurrency/Mutex.cpp
)
add_library(${THRIFT_LIBRARY} ${thriftcpp_SOURCES} ${thriftcpp_threads_SOURCES})
set_target_properties(${THRIFT_LIBRARY} PROPERTIES CXX_STANDARD 14) # REMOVE after https://github.com/apache/thrift/pull/1641
target_include_directories(${THRIFT_LIBRARY} SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src PRIVATE ${Boost_INCLUDE_DIRS})



# === arrow

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow)
# arrow/cpp/src/arrow/CMakeLists.txt
set(ARROW_SRCS
  ${LIBRARY_DIR}/array.cc

  ${LIBRARY_DIR}/builder.cc
  ${LIBRARY_DIR}/array/builder_adaptive.cc
  ${LIBRARY_DIR}/array/builder_base.cc
  ${LIBRARY_DIR}/array/builder_binary.cc
  ${LIBRARY_DIR}/array/builder_decimal.cc
  ${LIBRARY_DIR}/array/builder_dict.cc
  ${LIBRARY_DIR}/array/builder_nested.cc
  ${LIBRARY_DIR}/array/builder_primitive.cc

  ${LIBRARY_DIR}/buffer.cc
  ${LIBRARY_DIR}/compare.cc
  ${LIBRARY_DIR}/memory_pool.cc
  ${LIBRARY_DIR}/pretty_print.cc
  ${LIBRARY_DIR}/record_batch.cc
  ${LIBRARY_DIR}/status.cc
  ${LIBRARY_DIR}/table.cc
  ${LIBRARY_DIR}/table_builder.cc
  ${LIBRARY_DIR}/tensor.cc
  ${LIBRARY_DIR}/sparse_tensor.cc
  ${LIBRARY_DIR}/type.cc
  ${LIBRARY_DIR}/visitor.cc

  ${LIBRARY_DIR}/csv/converter.cc
  ${LIBRARY_DIR}/csv/chunker.cc
  ${LIBRARY_DIR}/csv/column-builder.cc
  ${LIBRARY_DIR}/csv/options.cc
  ${LIBRARY_DIR}/csv/parser.cc
  ${LIBRARY_DIR}/csv/reader.cc

  ${LIBRARY_DIR}/io/buffered.cc
  ${LIBRARY_DIR}/io/compressed.cc
  ${LIBRARY_DIR}/io/file.cc
  ${LIBRARY_DIR}/io/interfaces.cc
  ${LIBRARY_DIR}/io/memory.cc
  ${LIBRARY_DIR}/io/readahead.cc

  ${LIBRARY_DIR}/util/bit-util.cc
  ${LIBRARY_DIR}/util/compression.cc
  ${LIBRARY_DIR}/util/cpu-info.cc
  ${LIBRARY_DIR}/util/decimal.cc
  ${LIBRARY_DIR}/util/int-util.cc
  ${LIBRARY_DIR}/util/io-util.cc
  ${LIBRARY_DIR}/util/logging.cc
  ${LIBRARY_DIR}/util/key_value_metadata.cc
  ${LIBRARY_DIR}/util/task-group.cc
  ${LIBRARY_DIR}/util/thread-pool.cc
  ${LIBRARY_DIR}/util/trie.cc
  ${LIBRARY_DIR}/util/utf8.cc
)

set(ARROW_SRCS ${ARROW_SRCS}
    ${LIBRARY_DIR}/compute/context.cc
    ${LIBRARY_DIR}/compute/kernels/boolean.cc
    ${LIBRARY_DIR}/compute/kernels/cast.cc
    ${LIBRARY_DIR}/compute/kernels/hash.cc
    ${LIBRARY_DIR}/compute/kernels/util-internal.cc
)

if (LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
    set(ARROW_WITH_LZ4 1)
endif()

if(SNAPPY_INCLUDE_DIR AND SNAPPY_LIBRARY)
    set(ARROW_WITH_SNAPPY 1)
endif()

if(ZLIB_INCLUDE_DIR AND ZLIB_LIBRARIES)
    set(ARROW_WITH_ZLIB 1)
endif()

if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
    set(ARROW_WITH_ZSTD 1)
endif()

if (ARROW_WITH_LZ4)
  add_definitions(-DARROW_WITH_LZ4)
  SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_lz4.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_SNAPPY)
  add_definitions(-DARROW_WITH_SNAPPY)
  SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_snappy.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_ZLIB)
  add_definitions(-DARROW_WITH_ZLIB)
  SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_zlib.cc ${ARROW_SRCS})
endif()

if (ARROW_WITH_ZSTD)
  add_definitions(-DARROW_WITH_ZSTD)
  SET(ARROW_SRCS ${LIBRARY_DIR}/util/compression_zstd.cc ${ARROW_SRCS})
endif()


add_library(${ARROW_LIBRARY} ${ARROW_SRCS})
target_include_directories(${ARROW_LIBRARY} SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src ${Boost_INCLUDE_DIRS})
target_link_libraries(${ARROW_LIBRARY} PRIVATE ${DOUBLE_CONVERSION_LIBRARIES} Threads::Threads)
if (ARROW_WITH_LZ4)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${LZ4_LIBRARY})
endif()
if (ARROW_WITH_SNAPPY)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${SNAPPY_LIBRARY})
endif()
if (ARROW_WITH_ZLIB)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${ZLIB_LIBRARIES})
endif()
if (ARROW_WITH_ZSTD)
    target_link_libraries(${ARROW_LIBRARY} PRIVATE ${ZSTD_LIBRARY})
endif()


# === parquet

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/parquet)
# arrow/cpp/src/parquet/CMakeLists.txt
set(PARQUET_SRCS
  ${LIBRARY_DIR}/arrow/reader.cc
  ${LIBRARY_DIR}/arrow/record_reader.cc
  ${LIBRARY_DIR}/arrow/schema.cc
  ${LIBRARY_DIR}/arrow/writer.cc
  ${LIBRARY_DIR}/bloom_filter.cc
  ${LIBRARY_DIR}/column_reader.cc
  ${LIBRARY_DIR}/column_scanner.cc
  ${LIBRARY_DIR}/column_writer.cc
  ${LIBRARY_DIR}/file_reader.cc
  ${LIBRARY_DIR}/file_writer.cc
  ${LIBRARY_DIR}/metadata.cc
  ${LIBRARY_DIR}/murmur3.cc
  ${LIBRARY_DIR}/printer.cc
  ${LIBRARY_DIR}/schema.cc
  ${LIBRARY_DIR}/statistics.cc
  ${LIBRARY_DIR}/types.cc
  ${LIBRARY_DIR}/util/comparison.cc
  ${LIBRARY_DIR}/util/memory.cc
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

if(SANITIZE STREQUAL "undefined")
    target_compile_options(${PARQUET_LIBRARY} PRIVATE -fno-sanitize=undefined)
    target_compile_options(${ARROW_LIBRARY} PRIVATE -fno-sanitize=undefined)
endif()

# === tools

set(TOOLS_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/tools/parquet)
set(PARQUET_TOOLS parquet-dump-schema parquet-reader parquet-scan)
foreach(TOOL ${PARQUET_TOOLS})
    add_executable(${TOOL} ${TOOLS_DIR}/${TOOL}.cc)
    target_link_libraries(${TOOL} PRIVATE ${PARQUET_LIBRARY})
endforeach()
