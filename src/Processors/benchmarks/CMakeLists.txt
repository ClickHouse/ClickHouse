clickhouse_add_microbenchmark(benchmark_runtime_filter
    SOURCES benchmark_runtime_filter.cpp
    ../../Functions/ComparisonParams.cpp
    ../../Functions/equals.cpp
    ../../Functions/isDistinctFrom.cpp
    ../../Functions/isNotDistinctFrom.cpp
    ../../Functions/notEquals.cpp
    ../../Functions/registerFunctions.cpp
    LIBRARIES dbms)

