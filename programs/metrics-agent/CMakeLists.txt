# A small standalone monitoring agent: collects `AsynchronousMetrics` and sends them to ClickHouse over HTTP(S).
# It is a separate binary (not a part of the `clickhouse` multi-tool) and is built only on request:
#     ninja clickhouse-metrics-agent
#
# It links only `clickhouse_common_io`, and the linker takes from it only the objects that are used
# (about a hundred, starting from `AsynchronousMetrics.cpp`), so the binary stays small.

clickhouse_add_executable (clickhouse-metrics-agent MetricsAgent.cpp)
target_link_libraries (clickhouse-metrics-agent PRIVATE clickhouse_common_io ch_contrib::zstd)
set_target_properties (clickhouse-metrics-agent PROPERTIES
    EXCLUDE_FROM_ALL TRUE
    RUNTIME_OUTPUT_DIRECTORY ..)
