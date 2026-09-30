add_library (clickhouse_gpu_cudf STATIC
    Cudf.cu
)

gpu_island_target (clickhouse_gpu_cudf)

target_include_directories (clickhouse_gpu_cudf SYSTEM PRIVATE
    "${ClickHouse_SOURCE_DIR}/src"
    "${CONFIG_INCLUDE_PATH}"
)

target_link_libraries (clickhouse_gpu_cudf PRIVATE
    ch_contrib::cudf
    ch_contrib::rmm
    ch_gpu::cudart
)
