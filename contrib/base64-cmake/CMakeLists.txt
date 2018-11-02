SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/base64)

set(base64_compile_instructions "")
LIST(LENGTH base64_compile_instructions 0)
macro(cast_to_bool var instruction)
    if (HAVE_${var})
        set(base64_${var} 1)
        set(base64_${var}_opt ${instruction})
    else()
        set(base64_${var} 0)
    endif()
endmacro()

cast_to_bool(SSSE3 "-mssse3")
cast_to_bool(SSE41 "-msse4.1")
cast_to_bool(SSE42 "-msse4.2")
cast_to_bool(AVX   "-mavx")
cast_to_bool(AVX2  "-mavx2")

# write config.h file, to include it in application
file(READ config-header.tpl header)
file(WRITE config.h ${header})
file(APPEND config.h "#define HAVE_SSSE3                 ${base64_SSSE3}\n")
file(APPEND config.h "#define HAVE_SSE41                 ${base64_SSE41}\n")
file(APPEND config.h "#define HAVE_SSE42                 ${base64_SSE42}\n")
file(APPEND config.h "#define HAVE_AVX                   ${base64_AVX}\n")
file(APPEND config.h "#define HAVE_AVX2                  ${base64_AVX2}\n")

set(HAVE_FAST_UNALIGNED_ACCESS 0)
if (${base64_SSSE3} OR ${base64_SSE41} OR ${base64_SSE42} OR ${base64_AVX} OR ${base64_AVX2})
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

target_compile_options(base64 PRIVATE ${base64_SSSE3_opt} ${base64_SSE41_opt} ${base64_SSE42_opt} ${base64_AVX_opt} ${base64_AVX2_opt})
target_include_directories(base64 PRIVATE ${LIBRARY_DIR}/include .)