option (ENABLE_BASE64 "Enable base64" ${ENABLE_LIBRARIES})

if (NOT ENABLE_BASE64)
    message(STATUS "Not using base64")
    return()
endif()

SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aklomp-base64")

set(HAVE_NEON32 0)
set(HAVE_NEON64 0)
if (ARCH_AARCH64 AND NOT NO_ARMV81_OR_HIGHER)
    set(HAVE_NEON64 1)
endif ()

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

set(HAVE_FAST_UNALIGNED_ACCESS 0)
if(HAVE_SSSE3 OR HAVE_SSE41 OR HAVE_SSE42 OR HAVE_AVX OR HAVE_AVX2 OR HAVE_NEON64)
    set(HAVE_FAST_UNALIGNED_ACCESS 1)
endif ()

configure_file(config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

add_library(_base64
        # Library files
        "${LIBRARY_DIR}/lib/lib.c"
        "${LIBRARY_DIR}/lib/codec_choose.c"

        "${LIBRARY_DIR}/lib/tables/tables.c"

        # Codec implementations
        "${LIBRARY_DIR}/lib/arch/generic/codec.c"
        "${LIBRARY_DIR}/lib/arch/neon32/codec.c"
        "${LIBRARY_DIR}/lib/arch/neon64/codec.c"
        "${LIBRARY_DIR}/lib/arch/ssse3/codec.c"
        "${LIBRARY_DIR}/lib/arch/sse41/codec.c"
        "${LIBRARY_DIR}/lib/arch/sse42/codec.c"
        # ClickHouse does not support AVX* by default. We still need to compile these files
        # because the run-time codec detection requires functions from these file to be present.
        "${LIBRARY_DIR}/lib/arch/avx/codec.c"
        "${LIBRARY_DIR}/lib/arch/avx2/codec.c"
        "${LIBRARY_DIR}/lib/arch/avx512/codec.c"

        # Tables
        "${LIBRARY_DIR}/lib/tables/table_dec_32bit.h"
        "${LIBRARY_DIR}/lib/tables/table_enc_12bit.h"

        "${LIBRARY_DIR}/lib/codecs.h"
        "${CMAKE_CURRENT_BINARY_DIR}/config.h")

if(HAVE_AVX)
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/avx/codec.c PROPERTIES COMPILE_FLAGS -mavx)
endif()
if(HAVE_AVX2)
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/avx2/codec.c PROPERTIES COMPILE_FLAGS -mavx2)
endif()
if(HAVE_SSE41)
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/sse41/codec.c PROPERTIES COMPILE_FLAGS -msse4.1)
endif()
if(HAVE_SSE42)
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/sse42/codec.c PROPERTIES COMPILE_FLAGS -msse4.2)
endif()
if(HAVE_SSSE3)
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/ssse3/codec.c PROPERTIES COMPILE_FLAGS -mssse3)
endif()

target_include_directories(_base64 SYSTEM PUBLIC ${LIBRARY_DIR}/include)
target_include_directories(_base64 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

add_library(ch_contrib::base64 ALIAS _base64)
