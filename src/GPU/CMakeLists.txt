add_library (clickhouse_gpu_cudf STATIC
    Cudf.cu
    CudfReduction.cu
    CudfGroupBy.cu
    OffsetsKernels.cu
    BitUnpack.cu
    CudfHashJoin.cu
)

gpu_island_target (clickhouse_gpu_cudf)

target_compile_options (clickhouse_gpu_cudf PRIVATE --expt-extended-lambda)

target_include_directories (clickhouse_gpu_cudf SYSTEM PRIVATE
    "${ClickHouse_SOURCE_DIR}/src"
    "${CONFIG_INCLUDE_PATH}"
)

target_link_libraries (clickhouse_gpu_cudf PRIVATE
    ch_contrib::cudf
    ch_contrib::rmm
    ch_gpu::cudart
)

# The IVF index on the device, on cuVS, see CuvsIvf.cuh.
add_library (clickhouse_gpu_cuvs STATIC
    CuvsIvf.cu
)

gpu_island_target (clickhouse_gpu_cuvs)

target_compile_options (clickhouse_gpu_cuvs PRIVATE --expt-extended-lambda -DCUDA_API_PER_THREAD_DEFAULT_STREAM)

target_include_directories (clickhouse_gpu_cuvs SYSTEM PRIVATE
    "${ClickHouse_SOURCE_DIR}/src"
    "${CONFIG_INCLUDE_PATH}"
)

target_link_libraries (clickhouse_gpu_cuvs PRIVATE
    ch_contrib::cuvs
    ch_gpu::cudart
)
