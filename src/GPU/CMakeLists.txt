add_library (clickhouse_gpu_cudf STATIC
    Utils.cpp
    GPUAggregation.cpp
    GPUUploadPipe.cpp
    GPUHashJoin.cpp
)

gpu_island_target (clickhouse_gpu_cudf)

target_include_directories (clickhouse_gpu_cudf SYSTEM PRIVATE
    "${ClickHouse_SOURCE_DIR}/src"
    "${CONFIG_INCLUDE_PATH}"
)

target_link_libraries (clickhouse_gpu_cudf PUBLIC
    ch_contrib::cudf
    ch_contrib::nvcomp
    ch_gpu::cudart
)
