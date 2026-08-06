clickhouse_add_executable(runtime_filter
    runtime_filter.cpp
    ../../Functions/ComparisonParams.cpp
    ../../Functions/equals.cpp
    ../../Functions/isDistinctFrom.cpp
    ../../Functions/isNotDistinctFrom.cpp
    ../../Functions/notEquals.cpp
    ../../Functions/registerFunctions.cpp)
target_link_libraries(runtime_filter PRIVATE
    ch_contrib::gbenchmark_all
    dbms)

# This benchmark links a subset of dbms and is useful as a local diagnostic tool, but RelWithDebInfo
# debug sections make the binary multiple GiB. Keep the final target small regardless of build type.
# `-S` strips debug info and is accepted by GNU ld, lld, and Apple ld; use CMake's LINKER wrapper
# so the compiler driver receives the right platform-specific spelling.
target_link_options(runtime_filter PRIVATE "LINKER:-S")
