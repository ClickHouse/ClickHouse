add_library (clickhouse-copier-lib ClusterCopier.cpp)
target_link_libraries (clickhouse-copier-lib clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions)

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-copier clickhouse-copier.cpp)
    target_link_libraries (clickhouse-copier clickhouse-copier-lib)
endif ()
