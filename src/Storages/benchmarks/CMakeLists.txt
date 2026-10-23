clickhouse_add_executable(benchmark_statistics
    benchmark_statistics.cpp)
target_link_libraries(benchmark_statistics PRIVATE
    ch_contrib::gbenchmark_all
    dbms)

# This benchmark links a subset of dbms and is useful as a local diagnostic tool, but RelWithDebInfo
# debug sections make the binary multiple GiB. Keep the final target small regardless of build type.
# `-S` strips debug info and is accepted by GNU ld, lld, and Apple ld; use CMake's LINKER wrapper
# so the compiler driver receives the right platform-specific spelling.
target_link_options(benchmark_statistics PRIVATE "LINKER:-S")
