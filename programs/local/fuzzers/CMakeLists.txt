clickhouse_add_executable(clickhouse_fuzzer clickhouse_fuzzer.cpp)
target_include_directories (clickhouse_fuzzer PRIVATE ..)

target_link_libraries(clickhouse_fuzzer PRIVATE clickhouse-local-lib)

target_link_libraries(clickhouse_fuzzer PRIVATE common dbms)

if (TARGET ch_contrib::jemalloc)
    target_link_libraries(clickhouse_fuzzer PRIVATE ch_contrib::jemalloc)
endif()
