# Two targets, one per compiler, because the GPU engine spans two toolchains.
#
#   clickhouse_gpu_kernels  -- nvcc, gcc host pass, system libstdc++
#   clickhouse_gpu          -- clang, ClickHouse's own libc++ from contrib
#
# They are linked together through the C interface in GPUKernels.h. Keeping them as
# separate targets is what stops ClickHouse's global C++ flags, warning set and libc++
# include paths from reaching nvcc, where most of them are either rejected or wrong.

add_library (clickhouse_gpu_kernels STATIC
    kernels/Device.cu
    kernels/Arithmetic.cu
    kernels/Reduction.cu
)

# Drops everything the target inherited from ClickHouse's toolchain - see
# gpu_island_target in cmake/cuda.cmake for why. Must precede the target_* calls below.
gpu_island_target (clickhouse_gpu_kernels)

set_target_properties (clickhouse_gpu_kernels PROPERTIES CUDA_SEPARABLE_COMPILATION OFF)

# cudart_static keeps the CUDA runtime inside the binary, matching how ClickHouse links
# the rest of its dependencies. The CUDA driver (libcuda.so) is still resolved at run time
# from the host's NVIDIA installation -- it cannot be linked statically.
#
# Spelled out rather than taken from CUDA::cudart_static: cmake/cuda.cmake explains why
# FindCUDAToolkit is unusable here. Threads::Threads is ClickHouse's own definition from
# cmake/linux/default_libs.cmake.
target_link_libraries (clickhouse_gpu_kernels PRIVATE
    "${GPU_CUDART_LIBRARY}"
    "${GPU_CULIBOS_LIBRARY}"
    Threads::Threads
    ${CMAKE_DL_LIBS}
    rt
)

# The .cu files reach GPUKernels.h as "../GPUKernels.h", so no include path is needed for
# the kernels themselves -- which is the point of having cleared INCLUDE_DIRECTORIES.

add_library (clickhouse_gpu STATIC GPUDevice.cpp)

# Ordinary ClickHouse code: it keeps the global flags, warnings and libc++ that the
# kernels target had to shed.
target_include_directories (clickhouse_gpu PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/..")
target_link_libraries (clickhouse_gpu PUBLIC clickhouse_gpu_kernels)

# A standalone check that the two toolchains produce a working binary together. Plain
# add_executable rather than clickhouse_add_executable: this target has to stay buildable
# on its own, without the rest of the tree.
#
#     ninja gpu_smoke && ./src/GPU/gpu_smoke
if (ENABLE_TESTS)
    add_executable (gpu_smoke examples/gpu_smoke.cpp)
    target_link_libraries (gpu_smoke PRIVATE clickhouse_gpu)
endif ()
