# nanoarrow, the Arrow C data interface cuDF converts through.
#
# Plain C, so it needs no place on the cuDF island: C has a stable ABI and these objects
# link happily into either half. Only `common` is built, which is what upstream cuDF links
# too (nanoarrow_static, see cpp/cmake/thirdparty/get_nanoarrow.cmake). The `ipc` directory
# would drag in its own zstd and flatbuffers handling that ClickHouse already provides
# elsewhere, and `device` is not needed: cuDF includes nanoarrow_device.h for the
# ArrowDeviceArray layout, but the only device entry points it uses are the header's own
# static inline ArrowDeviceArrayMove and a release callback it defines itself. Leaving
# device out also keeps device/cuda.c and its CUDA driver calls (cuCtxPushCurrent,
# cuMemAllocAsync, ...) out of the link - those need libcuda, which would turn the driver
# into a load-time dependency of the server binary rather than something cudart dlopens.

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
)

# include/nanoarrow/nanoarrow_config.h is checked in, generated from the upstream
# nanoarrow_config.h.in.
target_include_directories (_nanoarrow SYSTEM PUBLIC
    "${ClickHouse_SOURCE_DIR}/contrib/nanoarrow/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

add_library (ch_contrib::nanoarrow ALIAS _nanoarrow)
