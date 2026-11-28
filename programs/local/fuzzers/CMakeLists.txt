clickhouse_add_executable(clickhouse_fuzzer clickhouse_fuzzer.cpp)
# Runs `clickhouse local` like `clickhouse_fuzzer`, with the hooks of `clickhouse_fuzzer.cpp` defined.
clickhouse_add_executable(mergetree_part_fuzzer clickhouse_fuzzer.cpp mergetree_part_fuzzer.cpp)

foreach (target clickhouse_fuzzer mergetree_part_fuzzer)
    target_include_directories (${target} PRIVATE ..)

    target_link_libraries(${target} PRIVATE clickhouse-local-lib)

    target_link_libraries(${target} PRIVATE common dbms)

    if (TARGET ch_contrib::jemalloc)
        target_link_libraries(${target} PRIVATE ch_contrib::jemalloc)
    endif()
endforeach()
