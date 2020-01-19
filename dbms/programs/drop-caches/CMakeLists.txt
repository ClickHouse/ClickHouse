set(CLICKHOUSE_DROP_CACHES_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/DropCaches.cpp)
set(CLICKHOUSE_DROP_CACHES_LINK PRIVATE clickhouse_common_io)

clickhouse_program_add(drop-caches)
