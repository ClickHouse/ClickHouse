
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

add_library(parquet_static STATIC ${PARQUET_SRCS})
add_library(parquet_shared SHARED ${PARQUET_SRCS})

target_include_directories(parquet_static PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)
target_include_directories(parquet_shared PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)

include(${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/ConfigureChecks.cmake)
#configure_file("${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/config.h.in" "${ClickHouse_BINARY_DIR}/contrib/thrift/thrift/config.h")

#target_link_libraries(parquet_static PRIVATE thrift_static)
#target_link_libraries(parquet_shared PRIVATE thrift)

target_include_directories(parquet_static PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src ${ClickHouse_BINARY_DIR}/contrib/thrift ${Boost_INCLUDE_DIRS})
target_include_directories(parquet_shared PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src ${ClickHouse_BINARY_DIR}/contrib/thrift ${Boost_INCLUDE_DIRS})



set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow)
# arrow/cpp/src/arrow/CMakeLists.txt
set(ARROW_SRCS
  ${LIBRARY_DIR}/array.cc
  ${LIBRARY_DIR}/buffer.cc
  ${LIBRARY_DIR}/builder.cc
  ${LIBRARY_DIR}/compare.cc
  ${LIBRARY_DIR}/memory_pool.cc
  ${LIBRARY_DIR}/pretty_print.cc
  ${LIBRARY_DIR}/record_batch.cc
  ${LIBRARY_DIR}/status.cc
  ${LIBRARY_DIR}/table.cc
  ${LIBRARY_DIR}/table_builder.cc
  ${LIBRARY_DIR}/tensor.cc
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
  ${LIBRARY_DIR}/util/io-util.cc
  ${LIBRARY_DIR}/util/logging.cc
  ${LIBRARY_DIR}/util/key_value_metadata.cc
  ${LIBRARY_DIR}/util/task-group.cc
  ${LIBRARY_DIR}/util/thread-pool.cc
  ${LIBRARY_DIR}/util/utf8.cc
)
#list(TRANSFORM ARROW_SRCS PREPEND ${LIBRARY_DIR}/) # cmake 3.12
add_library(arrow_static STATIC ${ARROW_SRCS})
add_library(arrow_shared SHARED ${ARROW_SRCS})
target_include_directories(arrow_static PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)
target_include_directories(arrow_shared PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)
target_include_directories(arrow_static PRIVATE ${Boost_INCLUDE_DIRS})
target_include_directories(arrow_shared PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(arrow_static PRIVATE ${DOUBLE_CONVERSION_LIBRARIES})
target_link_libraries(arrow_shared PRIVATE ${DOUBLE_CONVERSION_LIBRARIES})
