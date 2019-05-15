set(CLICKHOUSE_EXTRACT_FROM_CONFIG_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/ExtractFromConfig.cpp)
set(CLICKHOUSE_EXTRACT_FROM_CONFIG_LINK PRIVATE clickhouse_common_config clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
#set(CLICKHOUSE_EXTRACT_FROM_CONFIG_INCLUDE SYSTEM PRIVATE ...)

clickhouse_program_add(extract-from-config)
