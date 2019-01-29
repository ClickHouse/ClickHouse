SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/base64)

macro(cast_to_bool var instruction)
    if (HAVE_${var})
        set(base64_${var} 1)
        set(base64_${var}_opt ${instruction})
    else()
        set(base64_${var} 0)
    endif()
endmacro()

cast_to_bool(NEON32 "") # TODO flags
cast_to_bool(NEON64 "") # TODO flags
cast_to_bool(SSSE3 "-mssse3")
cast_to_bool(SSE41 "-msse4.1")
cast_to_bool(SSE42 "-msse4.2")
cast_to_bool(AVX   "-mavx")
cast_to_bool(AVX2  "-mavx2")

set(HAVE_FAST_UNALIGNED_ACCESS 0)
if(HAVE_SSSE3 OR HAVE_SSE41 OR HAVE_SSE42 OR HAVE_AVX OR HAVE_AVX2)
    set(HAVE_FAST_UNALIGNED_ACCESS 1)
endif ()

configure_file(config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

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
        ${CMAKE_CURRENT_BINARY_DIR}/config.h)

target_compile_options(base64 PRIVATE ${base64_SSSE3_opt} ${base64_SSE41_opt} ${base64_SSE42_opt} ${base64_AVX_opt} ${base64_AVX2_opt})
target_include_directories(base64 PRIVATE ${LIBRARY_DIR}/include ${CMAKE_CURRENT_BINARY_DIR})
