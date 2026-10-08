clickhouse_add_microbenchmark(integer_hash_tables_and_hashes
    SOURCES integer_hash_tables_and_hashes.cpp
    LIBRARIES
        clickhouse_common_io
        ch_contrib::wyhash
        ch_contrib::farmhash
        ch_contrib::xxHash)

clickhouse_add_microbenchmark(orc_string_dictionary
    SOURCES orc_string_dictionary.cpp
    LIBRARIES common)

clickhouse_add_microbenchmark(benchmark_radix_sort
    SOURCES radix_sort.cpp
    LIBRARIES dbms)

clickhouse_add_microbenchmark(wrap_in_nullable
    SOURCES wrap_in_nullable.cpp
    LIBRARIES dbms)

clickhouse_add_microbenchmark(profile_events
    SOURCES profile_events.cpp
    LIBRARIES clickhouse_common_io)

clickhouse_add_microbenchmark(benchmark_stack_trace
    SOURCES stack_trace.cpp
    LIBRARIES clickhouse_common_io)

clickhouse_add_microbenchmark(benchmark_sched_getcpu
    SOURCES sched_getcpu.cpp
    LIBRARIES ${CMAKE_DL_LIBS})

# A standalone model (not a Google Benchmark binary), so it is built only with ENABLE_BENCHMARKS.
if (ENABLE_BENCHMARKS)
    clickhouse_add_executable(hash_join_bandwidth_model
        hash_join_bandwidth_model.cpp
        hash_join_bench.cpp
        concurrent_hash_join_bench.cpp
        radix_hash_join_bench.cpp)
    target_link_libraries (hash_join_bandwidth_model PRIVATE
        dbms
        boost::program_options)
endif ()
