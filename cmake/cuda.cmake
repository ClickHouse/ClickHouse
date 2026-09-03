# The experimental GPU (CUDA) execution engine.

option (ENABLE_GPU "Enable the experimental GPU (CUDA) execution engine" OFF)

if (ENABLE_GPU)
    if (NOT OS_LINUX OR NOT ARCH_AMD64)
        message (FATAL_ERROR "ENABLE_GPU is only wired up for Linux x86_64 so far.")
    endif ()

    # nvcc's host pass uses system headers: contrib/sysroot ships no C++ stdlib and
    # crt/math_functions.h includes <cmath>. The link still resolves against the sysroot's
    # glibc 2.31, which works only because the C-ABI boundary keeps kernel host code to
    # libc calls far older than that. Verify after touching the kernels:
    #   nm -u src/GPU/libclickhouse_gpu_kernels.a | grep -E '__isoc2[0-9]_'
    # Do not set DISABLE_HERMETIC_BUILD: contrib hardcodes feature answers for the bundled
    # sysroot (every LIBURING_CONFIG_HAS_* is FALSE) and they go wrong on system headers.

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

    # The host pass needs the GNU dialect. RAPIDS code relies on GNU preprocessor
    # extensions: KvikIO dispatches its NVTX macros on `,##__VA_ARGS__`, which strict c++20
    # does not collapse, so the wrong overload is selected and the error surfaces three
    # headers away naming an unrelated function.
    #
    # It has to reach gcc through -Xcompiler. nvcc rejects `-std=gnu++20` for itself
    # ("Value 'gnu++20' is not defined for option 'std'"), and CMAKE_CUDA_EXTENSIONS is a
    # no-op here - CMake emits -std=c++20 for nvcc either way.
    string (APPEND CMAKE_CUDA_FLAGS " --expt-relaxed-constexpr -Xcompiler -fPIC -Xcompiler -std=gnu++20")

    set (CMAKE_CUDA_FLAGS_DEBUG          "-G -g")
    set (CMAKE_CUDA_FLAGS_RELWITHDEBINFO "-O3 -lineinfo")
    set (CMAKE_CUDA_FLAGS_RELEASE        "-O3")

    # Puts a target on the cuDF island: one libstdc++ shared with the device code, and none
    # of ClickHouse's own toolchain.
    #
    # Two separate problems. CMake has one CXX compiler per project, so these .cpp files
    # cannot go to gcc while ClickHouse's go to clang - compiling them as CUDA routes them
    # through nvcc's gcc host pass instead. And ClickHouse applies its toolchain to every
    # target in the tree via directory scope (`link_libraries(global-group)` in the
    # top-level CMakeLists.txt, `add_definitions` in cmake/target.cmake), which nvcc either
    # rejects - its `-Werror` takes a value, so it swallows the next argument - or accepts
    # to worse effect, pulling ClickHouse's libc++ headers into the island.
    #
    # Call this immediately after add_library, before any target_* call, or it clears those
    # too. And link ordinary ClickHouse targets as $<LINK_ONLY:...> with their include
    # directories given explicitly: they carry global-group's INTERFACE_COMPILE_OPTIONS,
    # which puts -Werror back on the island.
    function (gpu_island_target target)
        set_target_properties (${target} PROPERTIES
            LINK_LIBRARIES ""
            INTERFACE_LINK_LIBRARIES ""
            COMPILE_OPTIONS ""
            COMPILE_DEFINITIONS ""
            INCLUDE_DIRECTORIES ""
            POSITION_INDEPENDENT_CODE ON
        )

        get_target_property (_gpu_island_srcs ${target} SOURCES)
        foreach (_gpu_island_src IN LISTS _gpu_island_srcs)
            if (_gpu_island_src MATCHES "\\.(cpp|cc|cxx)$")
                set_source_files_properties ("${_gpu_island_src}"
                    TARGET_DIRECTORY ${target}
                    PROPERTIES LANGUAGE CUDA)
            endif ()
        endforeach ()
    endfunction ()

    message (STATUS "GPU engine: ENABLED")
    message (STATUS "  CUDA:          ${CMAKE_CUDA_COMPILER_VERSION} at ${GPU_CUDA_ROOT}")
    message (STATUS "  nvcc:          ${CMAKE_CUDA_COMPILER}")
    message (STATUS "  host compiler: ${CMAKE_CUDA_HOST_COMPILER}")
    message (STATUS "  architectures: ${CMAKE_CUDA_ARCHITECTURES}")
    message (STATUS "  cudart:        ${GPU_CUDART_LIBRARY}")
else ()
    message (STATUS "GPU engine: disabled (use -DENABLE_GPU=1)")
endif ()
