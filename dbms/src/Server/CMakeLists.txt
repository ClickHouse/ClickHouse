
add_library (clickhouse-extract-from-config-lib ${SPLIT_SHARED} ExtractFromConfig.cpp)
target_link_libraries (clickhouse-extract-from-config-lib clickhouse_common_config clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (clickhouse-benchmark-lib ${SPLIT_SHARED} Benchmark.cpp)
target_link_libraries (clickhouse-benchmark-lib clickhouse-client-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-benchmark-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (clickhouse-performance-test-lib ${SPLIT_SHARED} PerformanceTest.cpp)
target_link_libraries (clickhouse-performance-test-lib clickhouse_common_io dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-performance-test-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (clickhouse-format-lib ${SPLIT_SHARED} Format.cpp)
target_link_libraries (clickhouse-format-lib dbms clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (clickhouse-copier-lib ClusterCopier.cpp)
target_link_libraries (clickhouse-copier-lib clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions)

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-benchmark clickhouse-benchmark.cpp)
    target_link_libraries (clickhouse-benchmark clickhouse-benchmark-lib clickhouse_aggregate_functions)
endif ()
if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-performance-test clickhouse-performance-test.cpp)
    target_link_libraries (clickhouse-performance-test clickhouse-performance-test-lib dbms)
endif ()
if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-extract-from-config clickhouse-extract-from-config.cpp)
    target_link_libraries (clickhouse-extract-from-config clickhouse-extract-from-config-lib)
endif ()
if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-format clickhouse-format.cpp)
    target_link_libraries (clickhouse-format clickhouse-format-lib)
endif ()
if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-copier clickhouse-copier.cpp)
    target_link_libraries (clickhouse-copier clickhouse-copier-lib)
endif ()

