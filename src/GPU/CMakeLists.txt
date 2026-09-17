add_library (clickhouse_gpu_cudf STATIC GPUAggregationCudf.cpp)

gpu_island_target (clickhouse_gpu_cudf)

target_include_directories (clickhouse_gpu_cudf SYSTEM PRIVATE "${ClickHouse_SOURCE_DIR}/src")

target_link_libraries (clickhouse_gpu_cudf PUBLIC
    ch_contrib::cudf
    ch_contrib::nvcomp
    ch_gpu::cudart
)

add_library (clickhouse_gpu STATIC GPUAggregation.cpp)

target_link_libraries (clickhouse_gpu PUBLIC clickhouse_gpu_cudf)

target_link_libraries (clickhouse_gpu PRIVATE clickhouse_common_io)
