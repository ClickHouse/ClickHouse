clickhouse_add_executable(benchmark_cas_ref_protocol benchmark_cas_ref_protocol.cpp)
target_link_libraries (benchmark_cas_ref_protocol PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
