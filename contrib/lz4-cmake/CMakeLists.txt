# lz4 is the main compression method, cannot be disabled.

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/lz4")
set (SRCS
    "${LIBRARY_DIR}/lib/lz4.c"
    "${LIBRARY_DIR}/lib/lz4hc.c"
    "${LIBRARY_DIR}/lib/lz4frame.c"
    "${LIBRARY_DIR}/lib/xxhash.c"
)

add_library (_lz4 ${SRCS})
add_library (ch_contrib::lz4 ALIAS _lz4)

target_compile_definitions (_lz4 PUBLIC LZ4_DISABLE_DEPRECATE_WARNINGS=1)
target_compile_definitions (_lz4 PUBLIC LZ4_FAST_DEC_LOOP=1)

if(ARCH_S390X)
    target_compile_definitions(_lz4 PRIVATE LZ4_STATIC_LINKING_ONLY_ENDIANNESS_INDEPENDENT_OUTPUT)
endif()

if (SANITIZE STREQUAL "undefined")
    target_compile_options (_lz4 PRIVATE -fno-sanitize=undefined)
endif ()
target_include_directories(_lz4 PUBLIC "${LIBRARY_DIR}/lib")
