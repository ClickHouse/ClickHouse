
set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/parquet)

# arrow/cpp/src/parquet/CMakeLists.txt
set(PARQUET_SRCS
  arrow/reader.cc
  arrow/record_reader.cc
  arrow/schema.cc
  arrow/writer.cc
  bloom_filter.cc
  column_reader.cc
  column_scanner.cc
  column_writer.cc
  file_reader.cc
  file_writer.cc
  metadata.cc
  murmur3.cc
  printer.cc
  schema.cc
  statistics.cc
  types.cc
  util/comparison.cc
  util/memory.cc
)
list(TRANSFORM PARQUET_SRCS PREPEND ${LIBRARY_DIR}/)

list(APPEND PARQUET_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src/parquet/parquet_constants.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src/parquet/parquet_types.cpp
)

add_library(parquet_static STATIC ${PARQUET_SRCS})
add_library(parquet_shared SHARED ${PARQUET_SRCS})

target_include_directories(parquet_static PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)
target_include_directories(parquet_shared PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)

target_link_libraries(parquet_static PRIVATE thrift_static)
target_link_libraries(parquet_shared PRIVATE thrift)

target_include_directories(parquet_static PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src ${ClickHouse_BINARY_DIR}/contrib/thrift ${Boost_INCLUDE_DIRS})
target_include_directories(parquet_shared PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src ${ClickHouse_BINARY_DIR}/contrib/thrift ${Boost_INCLUDE_DIRS})



set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow)
# arrow/cpp/src/arrow/CMakeLists.txt
set(ARROW_SRCS
  array.cc
  buffer.cc
  builder.cc
  compare.cc
  memory_pool.cc
  pretty_print.cc
  record_batch.cc
  status.cc
  table.cc
  table_builder.cc
  tensor.cc
  type.cc
  visitor.cc

  csv/converter.cc
  csv/chunker.cc
  csv/column-builder.cc
  csv/options.cc
  csv/parser.cc
  csv/reader.cc

  io/buffered.cc
  io/compressed.cc
  io/file.cc
  io/interfaces.cc
  io/memory.cc
  io/readahead.cc

  util/bit-util.cc
  util/compression.cc
  util/cpu-info.cc
  util/decimal.cc
  util/io-util.cc
  util/logging.cc
  util/key_value_metadata.cc
  util/task-group.cc
  util/thread-pool.cc
  util/utf8.cc
)
list(TRANSFORM ARROW_SRCS PREPEND ${LIBRARY_DIR}/)
add_library(arrow_static STATIC ${ARROW_SRCS})
add_library(arrow_shared SHARED ${ARROW_SRCS})
target_include_directories(arrow_static PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)
target_include_directories(arrow_shared PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src ${CMAKE_CURRENT_SOURCE_DIR}/cpp/src)
target_include_directories(arrow_static PRIVATE ${Boost_INCLUDE_DIRS})
target_include_directories(arrow_shared PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(arrow_static PRIVATE ${DOUBLE_CONVERSION_LIBRARIES})
target_link_libraries(arrow_shared PRIVATE ${DOUBLE_CONVERSION_LIBRARIES})
