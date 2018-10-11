SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/base64)

# write config.h file, to include it in application
file(READ config-header.tpl header)
file(WRITE config.h ${header})
file(APPEND config.h "#define HAVE_SSE41                 ${HAVE_SSE41}\n")
file(APPEND config.h "#define HAVE_SSE42                 ${HAVE_SSE42}\n")

set(HAVE_FAST_UNALIGNED_ACCESS 0)
if (${HAVE_SSE41} OR ${HAVE_SSE42})
    set(HAVE_FAST_UNALIGNED_ACCESS 1)
endif ()

file(APPEND config.h "#define HAVE_FAST_UNALIGNED_ACCESS " ${HAVE_FAST_UNALIGNED_ACCESS} "\n")

add_library(base64 ${LINK_MODE}
        ${LIBRARY_DIR}/lib/lib.c
        ${LIBRARY_DIR}/lib/codec_choose.c
        ${LIBRARY_DIR}/lib/arch/avx/codec.c
        ${LIBRARY_DIR}/lib/arch/avx2/codec.c
        ${LIBRARY_DIR}/lib/arch/generic/codec.c
        ${LIBRARY_DIR}/lib/arch/neon32/codec.c
        ${LIBRARY_DIR}/lib/arch/neon64/codec.c
        ${LIBRARY_DIR}/lib/arch/sse41/codec.c
        ${LIBRARY_DIR}/lib/arch/sse42/codec.c
        ${LIBRARY_DIR}/lib/arch/ssse3/codec.c

        ${LIBRARY_DIR}/lib/codecs.h
        config.h)

target_include_directories(base64 PRIVATE ${LIBRARY_DIR}/include .)