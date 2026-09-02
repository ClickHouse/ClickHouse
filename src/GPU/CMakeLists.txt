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

# ClickHouse applies its toolchain to every target in the tree through directory scope:
# `link_libraries(global-group)` in the top-level CMakeLists.txt, `add_definitions` in
# cmake/target.cmake, `include_directories` for the generated config headers. All of it is
# written for clang and for the libc++ in contrib, and nvcc either rejects it outright
# (its own `-Werror` takes a value, so it swallows the following argument) or -- worse --
# accepts it: pulling ClickHouse's libc++ headers into a .cu file is precisely the ABI
# mixing that GPUKernels.h exists to prevent. So this target drops everything it inherited
# and states its dependencies itself.
#
# Clearing has to happen before target_link_libraries below, or it would wipe those too.
set_target_properties (clickhouse_gpu_kernels PROPERTIES
    LINK_LIBRARIES ""
    INTERFACE_LINK_LIBRARIES ""
    COMPILE_OPTIONS ""
    COMPILE_DEFINITIONS ""
    INCLUDE_DIRECTORIES ""
    CUDA_SEPARABLE_COMPILATION OFF
    POSITION_INDEPENDENT_CODE ON
)

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
