# Also in utils

set(CLICKHOUSE_COMPRESSOR_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Compressor.cpp)
set(CLICKHOUSE_COMPRESSOR_LINK PRIVATE clickhouse_compression clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
#set(CLICKHOUSE_COMPRESSOR_INCLUDE SYSTEM PRIVATE ...)

clickhouse_program_add(compressor)
