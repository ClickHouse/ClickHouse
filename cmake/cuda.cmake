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

    if (NOT DEFINED CMAKE_CUDA_COMPILER)
        find_program (GPU_NVCC
            NAMES nvcc
            HINTS "${CUDAToolkit_ROOT}" ENV CUDA_PATH ENV CUDA_HOME /usr/local/cuda
            PATH_SUFFIXES bin
            REQUIRED)
        set (CMAKE_CUDA_COMPILER "${GPU_NVCC}" CACHE FILEPATH "nvcc to compile .cu with")
    endif ()

    # <root>/bin/nvcc -> <root>
    get_filename_component (GPU_CUDA_ROOT "${CMAKE_CUDA_COMPILER}" DIRECTORY)
    get_filename_component (GPU_CUDA_ROOT "${GPU_CUDA_ROOT}" DIRECTORY)

    if (NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
        set (CMAKE_CUDA_ARCHITECTURES "75" CACHE STRING "CUDA architectures to generate code for")
    endif ()

    if (NOT DEFINED CMAKE_CUDA_HOST_COMPILER)
        find_program (GPU_CUDA_HOST_COMPILER NAMES g++ REQUIRED)
        set (CMAKE_CUDA_HOST_COMPILER "${GPU_CUDA_HOST_COMPILER}" CACHE FILEPATH "Host compiler for nvcc")
    endif ()

    set (_ch_saved_try_compile_target_type "${CMAKE_TRY_COMPILE_TARGET_TYPE}")
    set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

    enable_language (CUDA)

    set (CMAKE_TRY_COMPILE_TARGET_TYPE "${_ch_saved_try_compile_target_type}")

    if (NOT CMAKE_CUDA_COMPILER_ID STREQUAL "NVIDIA")
        message (FATAL_ERROR "The GPU engine expects nvcc, got ${CMAKE_CUDA_COMPILER_ID}.")
    endif ()

    set (GPU_CUDA_MINIMUM_VERSION 13.0)
    if (CMAKE_CUDA_COMPILER_VERSION VERSION_LESS ${GPU_CUDA_MINIMUM_VERSION})
        message (FATAL_ERROR
            "CUDA ${CMAKE_CUDA_COMPILER_VERSION} is unsupported, the minimum required version is "
            "${GPU_CUDA_MINIMUM_VERSION}.")
    endif ()

    find_library (GPU_CUDART_LIBRARY
        NAMES cudart_static
        HINTS "${GPU_CUDA_ROOT}"
        PATH_SUFFIXES lib64 lib
        REQUIRED)
    find_library (GPU_CULIBOS_LIBRARY
        NAMES culibos
        HINTS "${GPU_CUDA_ROOT}"
        PATH_SUFFIXES lib64 lib
        REQUIRED)
    mark_as_advanced (GPU_CUDART_LIBRARY GPU_CULIBOS_LIBRARY)

    set (CMAKE_CUDA_RUNTIME_LIBRARY None)

    set (CMAKE_CUDA_STANDARD 20)
    set (CMAKE_CUDA_STANDARD_REQUIRED ON)

    string (APPEND CMAKE_CUDA_FLAGS " --expt-relaxed-constexpr -Xcompiler -fPIC")

    set (CMAKE_CUDA_FLAGS_DEBUG          "-G -g")
    set (CMAKE_CUDA_FLAGS_RELWITHDEBINFO "-O3 -lineinfo")
    set (CMAKE_CUDA_FLAGS_RELEASE        "-O3")

    message (STATUS "GPU engine: ENABLED")
    message (STATUS "  CUDA:          ${CMAKE_CUDA_COMPILER_VERSION} at ${GPU_CUDA_ROOT}")
    message (STATUS "  nvcc:          ${CMAKE_CUDA_COMPILER}")
    message (STATUS "  host compiler: ${CMAKE_CUDA_HOST_COMPILER}")
    message (STATUS "  architectures: ${CMAKE_CUDA_ARCHITECTURES}")
    message (STATUS "  cudart:        ${GPU_CUDART_LIBRARY}")
else ()
    message (STATUS "GPU engine: disabled (use -DENABLE_GPU=1 -DDISABLE_HERMETIC_BUILD=1)")
endif ()
