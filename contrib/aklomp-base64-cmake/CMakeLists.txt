option (ENABLE_BASE64 "Enable base64" ${ENABLE_LIBRARIES})

if (NOT ENABLE_BASE64)
    message(STATUS "Not using base64")
    return()
endif()

SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aklomp-base64")

if (ARCH_AMD64)
    # These defines enable/disable SIMD codecs in base64's runtime codec dispatch.
    # We don't want to limit ourselves --> enable all.
    set(HAVE_SSSE3 1)
    set(HAVE_SSE41 1)
    set(HAVE_SSE42 1)
    set(HAVE_AVX 1)
    set(HAVE_AVX2 1)
    set(HAVE_AVX512 1)
endif ()

if (ARCH_AARCH64)
    # The choice of HAVE_NEON* depends on the target machine because base64 provides
    # no runtime dispatch on ARM. NEON is only mandatory with the normal build profile.
    if(NOT NO_ARMV81_OR_HIGHER)
        set(HAVE_NEON64 1)
        set(HAVE_NEON32 0)
    endif ()
endif ()

configure_file(config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

add_library(_base64
    "${LIBRARY_DIR}/lib/lib.c"
    "${LIBRARY_DIR}/lib/codec_choose.c"

    "${LIBRARY_DIR}/lib/tables/tables.c"
    "${LIBRARY_DIR}/lib/tables/table_dec_32bit.h"
    "${LIBRARY_DIR}/lib/tables/table_enc_12bit.h"

    "${LIBRARY_DIR}/lib/codecs.h"

    "${CMAKE_CURRENT_BINARY_DIR}/config.h"

    "${LIBRARY_DIR}/lib/arch/generic/codec.c"
    "${LIBRARY_DIR}/lib/arch/ssse3/codec.c"
    "${LIBRARY_DIR}/lib/arch/sse41/codec.c"
    "${LIBRARY_DIR}/lib/arch/sse42/codec.c"
    "${LIBRARY_DIR}/lib/arch/avx/codec.c"
    "${LIBRARY_DIR}/lib/arch/avx2/codec.c"
    "${LIBRARY_DIR}/lib/arch/avx512/codec.c"

    "${LIBRARY_DIR}/lib/arch/neon32/codec.c"
    "${LIBRARY_DIR}/lib/arch/neon64/codec.c"
)

if (ARCH_AMD64)
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/ssse3/codec.c PROPERTIES COMPILE_FLAGS "-mssse3")
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/sse41/codec.c PROPERTIES COMPILE_FLAGS "-msse4.1")
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/sse42/codec.c PROPERTIES COMPILE_FLAGS "-msse4.2")
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/avx/codec.c PROPERTIES COMPILE_FLAGS "-mavx")
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/avx2/codec.c PROPERTIES COMPILE_FLAGS "-mavx2")
    set_source_files_properties(${LIBRARY_DIR}/lib/arch/avx512/codec.c PROPERTIES COMPILE_FLAGS "-mavx512vl -mavx512vbmi")
endif()

target_include_directories(_base64 SYSTEM PUBLIC ${LIBRARY_DIR}/include)
target_include_directories(_base64 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

add_library(ch_contrib::base64 ALIAS _base64)
