# The experimental GPU (CUDA) execution engine.

option (ENABLE_GPU "Enable the experimental GPU (CUDA) execution engine" OFF)

if (ENABLE_GPU)
    if (NOT OS_LINUX OR NOT ARCH_AMD64)
        message (FATAL_ERROR "ENABLE_GPU is only wired up for Linux x86_64 so far.")
    endif ()

    if (NOT DISABLE_HERMETIC_BUILD)
        message (FATAL_ERROR
            "ENABLE_GPU requires -DDISABLE_HERMETIC_BUILD=1. nvcc compiles its host pass against "
            "the system libc, which cannot be linked against the glibc in contrib/sysroot.")
    endif ()

    find_package (CUDAToolkit 13.0 REQUIRED)

    if (NOT DEFINED CMAKE_CUDA_COMPILER)
        set (CMAKE_CUDA_COMPILER "${CUDAToolkit_NVCC_EXECUTABLE}" CACHE FILEPATH "nvcc to compile .cu with")
    endif ()

    if (NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
        set (CMAKE_CUDA_ARCHITECTURES "75" CACHE STRING "CUDA architectures to generate code for")
    endif ()

    if (NOT DEFINED CMAKE_CUDA_HOST_COMPILER)
        find_program (GPU_CUDA_HOST_COMPILER NAMES g++ REQUIRED)
        set (CMAKE_CUDA_HOST_COMPILER "${GPU_CUDA_HOST_COMPILER}" CACHE FILEPATH "Host compiler for nvcc")
    endif ()

    enable_language (CUDA)

    if (NOT CMAKE_CUDA_COMPILER_ID STREQUAL "NVIDIA")
        message (FATAL_ERROR "The GPU engine expects nvcc, got ${CMAKE_CUDA_COMPILER_ID}.")
    endif ()

    set (CMAKE_CUDA_STANDARD 20)
    set (CMAKE_CUDA_STANDARD_REQUIRED ON)

    string (APPEND CMAKE_CUDA_FLAGS " --expt-relaxed-constexpr -Xcompiler -fPIC")

    set (CMAKE_CUDA_FLAGS_DEBUG          "-G -g")
    set (CMAKE_CUDA_FLAGS_RELWITHDEBINFO "-O3 -lineinfo")
    set (CMAKE_CUDA_FLAGS_RELEASE        "-O3")

    message (STATUS "GPU engine: ENABLED")
    message (STATUS "  CUDA toolkit:  ${CUDAToolkit_VERSION} at ${CUDAToolkit_LIBRARY_ROOT}")
    message (STATUS "  nvcc:          ${CMAKE_CUDA_COMPILER}")
    message (STATUS "  host compiler: ${CMAKE_CUDA_HOST_COMPILER}")
    message (STATUS "  architectures: ${CMAKE_CUDA_ARCHITECTURES}")
else ()
    message (STATUS "GPU engine: disabled (use -DENABLE_GPU=1 -DDISABLE_HERMETIC_BUILD=1)")
endif ()
