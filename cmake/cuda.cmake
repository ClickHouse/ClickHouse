# The experimental GPU (CUDA) execution engine.

option (ENABLE_GPU "Enable the experimental GPU (CUDA) execution engine" OFF)

if (ENABLE_GPU)
    if (NOT OS_LINUX OR NOT ARCH_AMD64)
        message (FATAL_ERROR "ENABLE_GPU is only wired up for Linux x86_64 so far.")
    endif ()

    # Not a porting gap - the two cannot coexist. USE_MUSL links -static, and CUDA reaches
    # its driver by dlopen'ing libcuda.so.1, which musl's static dlopen refuses outright
    # ("Dynamic loading not supported"). libcudart_static.a also needs dlmopen, dlvsym and
    # gnu_get_libc_version, none of which musl has. And libcuda.so.1 is NVIDIA's closed
    # binary linked against glibc, so a musl process could not load it either way. A musl
    # build wanting GPU work has to put it behind a separate glibc process.
    if (USE_MUSL)
        message (FATAL_ERROR "ENABLE_GPU is incompatible with USE_MUSL. See cmake/cuda.cmake.")
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

    # Both filled in from nvcc itself during enable_language, so distro layouts come out
    # right: a packaged nvcc lives in /usr/bin, where stripping `bin/` would name /usr and
    # miss archives that sit in the multiarch libdir. The implicit dirs also carry
    # lib64/stubs, which is where the driver stub would be found if anything needed it.
    set (GPU_CUDA_ROOT "${CMAKE_CUDA_COMPILER_TOOLKIT_ROOT}")
    set (GPU_CUDA_LIBRARY_DIRS
        ${CMAKE_CUDA_HOST_IMPLICIT_LINK_DIRECTORIES}
        "${GPU_CUDA_ROOT}/lib64"
        "${GPU_CUDA_ROOT}/lib")
    list (REMOVE_DUPLICATES GPU_CUDA_LIBRARY_DIRS)

    # Named by explicit path, not searched. ClickHouse forbids find_library in
    # CMakeLists.txt (ci/jobs/scripts/check_style/check_cpp.sh) so a build cannot pick up
    # whatever a machine happens to carry, and the same reasoning applies here even though
    # this file is not itself scanned. The candidate dirs are the ones nvcc reports, so
    # every supported layout is covered by existence rather than by search.
    function (gpu_cuda_library var basename)
        foreach (_dir IN LISTS GPU_CUDA_LIBRARY_DIRS)
            if (EXISTS "${_dir}/lib${basename}.a")
                set (${var} "${_dir}/lib${basename}.a" PARENT_SCOPE)
                return ()
            endif ()
        endforeach ()
        message (FATAL_ERROR
            "lib${basename}.a is missing from the CUDA toolkit at ${GPU_CUDA_ROOT}. "
            "Looked in: ${GPU_CUDA_LIBRARY_DIRS}")
    endfunction ()

    # The static runtime, so the CUDA runtime lands inside the binary like the rest of
    # ClickHouse's dependencies. The driver (libcuda.so.1) stays a run-time dependency: it
    # is dlopen'd by cudart and cannot be linked statically.
    gpu_cuda_library (GPU_CUDART_LIBRARY         cudart_static)
    gpu_cuda_library (GPU_CULIBOS_LIBRARY        culibos)

    # NVRTC and nvJitLink are how librtcx compiles cuDF's embedded CUDA fragments at query
    # time, which makes them run-time requirements as much as build-time ones.
    gpu_cuda_library (GPU_NVRTC_LIBRARY          nvrtc_static)
    gpu_cuda_library (GPU_NVRTC_BUILTINS_LIBRARY nvrtc-builtins_static)
    gpu_cuda_library (GPU_NVJITLINK_LIBRARY      nvJitLink_static)
    gpu_cuda_library (GPU_NVPTXCOMPILER_LIBRARY  nvptxcompiler_static)

    # What FindCUDAToolkit would have attached to CUDA::cudart_static. Consumers link this
    # rather than naming the archive, or the first executable to pull one in fails on
    # pthread_*, dl* and clock_gettime. Threads::Threads arrives with
    # cmake/linux/default_libs.cmake, later than this file - fine, it is resolved at
    # generate time.
    add_library (ch_gpu::cudart INTERFACE IMPORTED GLOBAL)
    set_target_properties (ch_gpu::cudart PROPERTIES INTERFACE_LINK_LIBRARIES
        "${GPU_CUDART_LIBRARY};${GPU_CULIBOS_LIBRARY};Threads::Threads;${CMAKE_DL_LIBS};rt")

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
