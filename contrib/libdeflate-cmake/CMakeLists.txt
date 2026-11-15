option (ENABLE_LIBDEFLATE "Enable libdeflate (fast one-shot DEFLATE/zlib/gzip codec)" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBDEFLATE)
    message(STATUS "Not using libdeflate")
    return()
endif()

set(LIBDEFLATE_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libdeflate")

# Architecture-specific code is pulled in via headers from these translation units, so the same
# source list builds on every target; the per-arch cpu_features.c files compile to nothing on a
# foreign architecture.
set(SRCS
    "${LIBDEFLATE_SOURCE_DIR}/lib/utils.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/arm/cpu_features.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/x86/cpu_features.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/deflate_compress.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/deflate_decompress.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/adler32.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/zlib_compress.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/zlib_decompress.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/crc32.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/gzip_compress.c"
    "${LIBDEFLATE_SOURCE_DIR}/lib/gzip_decompress.c"
)

add_library(_libdeflate ${SRCS})
add_library(ch_contrib::libdeflate ALIAS _libdeflate)

target_include_directories(_libdeflate SYSTEM BEFORE PUBLIC "${LIBDEFLATE_SOURCE_DIR}")
