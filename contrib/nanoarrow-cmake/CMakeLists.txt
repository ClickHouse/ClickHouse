# nanoarrow, the Arrow C data interface cuDF converts through.
#
# Plain C, so it needs no place on the cuDF island: C has a stable ABI and these objects
# link happily into either half. Only the `common` and `device` parts are built - cuDF
# includes nanoarrow.h, nanoarrow.hpp and nanoarrow_device.h, and the `ipc` directory would
# drag in its own zstd and flatbuffers handling that ClickHouse already provides elsewhere.

if (NOT ENABLE_GPU)
    message (STATUS "Not using nanoarrow")
    return ()
endif ()

set (NANOARROW_DIR "${ClickHouse_SOURCE_DIR}/contrib/nanoarrow/src/nanoarrow")

add_library (_nanoarrow
    "${NANOARROW_DIR}/common/array.c"
    "${NANOARROW_DIR}/common/array_stream.c"
    "${NANOARROW_DIR}/common/schema.c"
    "${NANOARROW_DIR}/common/utils.c"
    "${NANOARROW_DIR}/device/device.c"
    "${NANOARROW_DIR}/device/cuda.c"
)

# include/nanoarrow/nanoarrow_config.h is checked in, generated from the upstream
# nanoarrow_config.h.in. device/cuda.c needs the CUDA driver header.
target_include_directories (_nanoarrow SYSTEM PUBLIC
    "${ClickHouse_SOURCE_DIR}/contrib/nanoarrow/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_include_directories (_nanoarrow SYSTEM PRIVATE "${GPU_CUDA_ROOT}/include")
target_compile_definitions (_nanoarrow PUBLIC NANOARROW_DEVICE_WITH_CUDA)
target_link_libraries (_nanoarrow PRIVATE "${GPU_CUDART_LIBRARY}")

add_library (ch_contrib::nanoarrow ALIAS _nanoarrow)
