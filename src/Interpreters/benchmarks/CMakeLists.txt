clickhouse_add_executable(hash_join_probe_loop hash_join_probe_loop.cpp)
target_link_libraries (hash_join_probe_loop PRIVATE
    ch_contrib::gbenchmark_all
    dbms)
