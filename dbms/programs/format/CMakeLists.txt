set(CLICKHOUSE_FORMAT_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Format.cpp)
set(CLICKHOUSE_FORMAT_LINK PRIVATE dbms clickhouse_common_io clickhouse_parsers ${Boost_PROGRAM_OPTIONS_LIBRARY})
#set(CLICKHOUSE_FORMAT_INCLUDE SYSTEM PRIVATE ...)

clickhouse_program_add(format)
