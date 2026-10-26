# KvikIO, the file IO layer cuDF reads Parquet and ORC through.
#
# Remote IO is left out: it wants libcurl, which ClickHouse does not carry in contrib, and
# ClickHouse has its own S3 and HTTP stack anyway. That drops the whole KvikIO_REMOTE_SUPPORT
# block - hdfs.cpp, remote_handle.cpp, shim/libcurl.cpp and detail/{multi_poll_reactor,
# remote_callback, tls, url}.cpp - so the list below is upstream's base set verbatim.
#
# cuFile (GPUDirect Storage) is compiled in from the toolkit's cufile.h, but the library
# itself is only dlopen'd at run time, so a host without the cuFile driver still runs the
# POSIX IO path.

if (NOT ENABLE_GPU)
    message (STATUS "Not using KvikIO")
    return ()
endif ()

set (KVIKIO_DIR "${ClickHouse_SOURCE_DIR}/contrib/kvikio/cpp")

add_library (_kvikio
    "${KVIKIO_DIR}/src/batch.cpp"
    "${KVIKIO_DIR}/src/bounce_buffer.cpp"
    "${KVIKIO_DIR}/src/buffer.cpp"
    "${KVIKIO_DIR}/src/compat_mode.cpp"
    "${KVIKIO_DIR}/src/compat_mode_manager.cpp"
    "${KVIKIO_DIR}/src/defaults.cpp"
    "${KVIKIO_DIR}/src/error.cpp"
    "${KVIKIO_DIR}/src/file_handle.cpp"
    "${KVIKIO_DIR}/src/file_utils.cpp"
    "${KVIKIO_DIR}/src/http_status_codes.cpp"
    "${KVIKIO_DIR}/src/logger.cpp"
    "${KVIKIO_DIR}/src/mmap.cpp"
    "${KVIKIO_DIR}/src/stream.cpp"
    "${KVIKIO_DIR}/src/utils.cpp"
    "${KVIKIO_DIR}/src/cufile/config.cpp"
    "${KVIKIO_DIR}/src/cufile/driver.cpp"
    "${KVIKIO_DIR}/src/detail/bounce_buffer_cache.cpp"
    "${KVIKIO_DIR}/src/detail/concurrent_request_limiter.cpp"
    "${KVIKIO_DIR}/src/detail/env.cpp"
    "${KVIKIO_DIR}/src/detail/event.cpp"
    "${KVIKIO_DIR}/src/detail/nvtx.cpp"
    "${KVIKIO_DIR}/src/detail/posix_io.cpp"
    "${KVIKIO_DIR}/src/detail/stream.cpp"
    "${KVIKIO_DIR}/src/detail/utils.cpp"
    "${KVIKIO_DIR}/src/shim/cuda.cpp"
    "${KVIKIO_DIR}/src/shim/cufile.cpp"
    "${KVIKIO_DIR}/src/shim/utils.cpp"
)

gpu_island_target (_kvikio)

# include/kvikio/logger_macros.hpp is checked in, generated from rapids-logger's template
# the way contrib/sz3-cmake keeps its version.hpp.
target_include_directories (_kvikio SYSTEM PUBLIC
    "${KVIKIO_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${GPU_CUDA_ROOT}/include"
)

target_compile_definitions (_kvikio
    PRIVATE KVIKIO_LOG_ACTIVE_LEVEL=RAPIDS_LOGGER_LOG_LEVEL_INFO
)

# The cuFile feature defines, gated the way upstream gates them: KVIKIO_CUFILE_FOUND only
# where cufile.h is actually there, and KVIKIO_CUFILE_VERSION_API_FOUND only where that
# header declares `cuFileGetVersion` (cuFile 1.7, CUDA 12.2). The version API is a separate
# feature, not an implication of having the header: without the define
# shim/cufile_h_wrapper.hpp declares the symbol itself and src/shim/cufile.cpp reports
# version 1070, so older cuFile headers keep working - defining it unconditionally would
# break them.
#
# Upstream probes each API with `try_compile`, which ClickHouse forbids (see
# cmake/block_build_time_checks.cmake), so the declarations are looked up in the very header
# that gets compiled - no compiler run, and the answer follows the toolchain in use. No
# cuFile library is linked either way: KvikIO dlopen's it.
set (KVIKIO_CUFILE_HEADER "${GPU_CUDA_ROOT}/include/cufile.h")
if (EXISTS "${KVIKIO_CUFILE_HEADER}")
    file (STRINGS "${KVIKIO_CUFILE_HEADER}" KVIKIO_CUFILE_BATCH_API_DECLARATION
        REGEX "CUfileError_t[ \t]+cuFileBatchIOSetUp[ \t]*\\(")
    file (STRINGS "${KVIKIO_CUFILE_HEADER}" KVIKIO_CUFILE_STREAM_API_DECLARATION
        REGEX "CUfileError_t[ \t]+cuFileReadAsync[ \t]*\\(")
    file (STRINGS "${KVIKIO_CUFILE_HEADER}" KVIKIO_CUFILE_VERSION_API_DECLARATION
        REGEX "CUfileError_t[ \t]+cuFileGetVersion[ \t]*\\(")

    # Upstream raises the same two as fatal: the batch and stream entry points are named
    # unconditionally by shim/cufile.hpp, so a header without them cannot build KvikIO.
    if (NOT KVIKIO_CUFILE_BATCH_API_DECLARATION)
        message (FATAL_ERROR "Missing cuFile Batch API: ${KVIKIO_CUFILE_HEADER} does not "
            "declare cuFileBatchIOSetUp")
    endif ()
    if (NOT KVIKIO_CUFILE_STREAM_API_DECLARATION)
        message (FATAL_ERROR "Missing cuFile Stream API: ${KVIKIO_CUFILE_HEADER} does not "
            "declare cuFileReadAsync")
    endif ()

    message (STATUS "Using KvikIO with cuFile (GPUDirect Storage)")
    target_compile_definitions (_kvikio PUBLIC KVIKIO_CUFILE_FOUND)

    if (KVIKIO_CUFILE_VERSION_API_DECLARATION)
        message (STATUS "Using KvikIO with the cuFile version API")
        target_compile_definitions (_kvikio PUBLIC KVIKIO_CUFILE_VERSION_API_FOUND)
    else ()
        message (STATUS "Using KvikIO without the cuFile version API: ${KVIKIO_CUFILE_HEADER} "
            "does not declare cuFileGetVersion, the cuFile version will be reported as 1070")
    endif ()
else ()
    # Upstream degrades to POSIX IO here, but only for the part of KvikIO that is guarded:
    # cufile_h_wrapper.hpp's fallback declares the plain read/write/driver entry points and
    # nothing else, while shim/cufile.hpp takes `decltype(cuFileBatchIOSetUp)` and
    # `decltype(cuFileReadAsync)`, and batch.hpp stores a `CUfileBatchHandle_t`, both
    # outside any guard. So at this pin the header is a build requirement, and saying so at
    # configure time beats several hundred lines of missing-declaration errors.
    message (FATAL_ERROR
        "cufile.h is missing from the CUDA toolkit at ${GPU_CUDA_ROOT} (looked for "
        "${KVIKIO_CUFILE_HEADER}). It comes with the full CUDA toolkit; on a split install "
        "it is in the libcufile development package. KvikIO needs it to compile - see "
        "contrib/kvikio-cmake/CMakeLists.txt.")
endif ()

target_link_libraries (_kvikio PUBLIC
    ch_contrib::cccl
    ch_contrib::rapids_logger
    ch_contrib::nvtx3
    ch_contrib::bs_thread_pool
    ch_gpu::cudart
)

add_library (ch_contrib::kvikio ALIAS _kvikio)
