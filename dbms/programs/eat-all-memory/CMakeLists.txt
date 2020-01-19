set(CLICKHOUSE_EAT_ALL_MEMORY_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/EatAllMemory.cpp)
set(CLICKHOUSE_EAT_ALL_MEMORY_LINK PRIVATE clickhouse_common_io)

clickhouse_program_add(eat-all-memory)
