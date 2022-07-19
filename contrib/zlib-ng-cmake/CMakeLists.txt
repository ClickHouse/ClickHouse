set (SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/zlib-ng)

add_definitions(-DZLIB_COMPAT)
add_definitions(-DWITH_GZFILEOP)
add_definitions(-DUNALIGNED_OK)
add_definitions(-DUNALIGNED64_OK)

set (HAVE_UNISTD_H 1)
add_definitions(-D_LARGEFILE64_SOURCE=1 -D__USE_LARGEFILE64)

add_definitions(-DHAVE_VISIBILITY_HIDDEN)
add_definitions(-DHAVE_VISIBILITY_INTERNAL)
add_definitions(-DHAVE_BUILTIN_CTZ)
add_definitions(-DHAVE_BUILTIN_CTZLL)

set(ZLIB_ARCH_SRCS)
set(ZLIB_ARCH_HDRS)

set(ARCHDIR "arch/generic")

if(ARCH_AARCH64)
    set(ARCHDIR "${SOURCE_DIR}/arch/arm")

    add_definitions(-DARM_FEATURES)
    add_definitions(-DARM_AUXV_HAS_CRC32 -DARM_ASM_HWCAP)
    add_definitions(-DARM_AUXV_HAS_NEON)
    add_definitions(-DARM_ACLE_CRC_HASH)
    add_definitions(-DARM_NEON_ADLER32 -DARM_NEON_CHUNKSET -DARM_NEON_SLIDEHASH)

    list(APPEND ZLIB_ARCH_HDRS ${ARCHDIR}/arm.h)
    list(APPEND ZLIB_ARCH_SRCS ${ARCHDIR}/armfeature.c)
    set(ACLE_SRCS ${ARCHDIR}/crc32_acle.c ${ARCHDIR}/insert_string_acle.c)
    list(APPEND ZLIB_ARCH_SRCS ${ACLE_SRCS})
    set(NEON_SRCS ${ARCHDIR}/adler32_neon.c ${ARCHDIR}/chunkset_neon.c ${ARCHDIR}/slide_neon.c)
    list(APPEND ZLIB_ARCH_SRCS ${NEON_SRCS})

elseif(ARCH_PPC64LE)
    set(ARCHDIR "${SOURCE_DIR}/arch/power")

    add_definitions(-DPOWER8)
    add_definitions(-DPOWER_FEATURES)
    add_definitions(-DPOWER8_VSX_ADLER32)
    add_definitions(-DPOWER8_VSX_SLIDEHASH)

    list(APPEND ZLIB_ARCH_HDRS ${ARCHDIR}/power.h)
    list(APPEND ZLIB_ARCH_SRCS ${ARCHDIR}/power.c)
    set(POWER8_SRCS ${ARCHDIR}/adler32_power8.c ${ARCHDIR}/slide_hash_power8.c)
    list(APPEND ZLIB_ARCH_SRCS ${POWER8_SRCS})

elseif(ARCH_AMD64)
    set(ARCHDIR "${SOURCE_DIR}/arch/x86")

    add_definitions(-DX86_FEATURES)
    list(APPEND ZLIB_ARCH_HDRS ${ARCHDIR}/x86.h)
    list(APPEND ZLIB_ARCH_SRCS ${ARCHDIR}/x86.c)
    if(ENABLE_AVX2)
        add_definitions(-DX86_AVX2 -DX86_AVX2_ADLER32 -DX86_AVX_CHUNKSET)
        set(AVX2_SRCS ${ARCHDIR}/slide_avx.c)
        list(APPEND AVX2_SRCS ${ARCHDIR}/chunkset_avx.c)
        list(APPEND AVX2_SRCS ${ARCHDIR}/compare258_avx.c)
        list(APPEND AVX2_SRCS ${ARCHDIR}/adler32_avx.c)
        list(APPEND ZLIB_ARCH_SRCS ${AVX2_SRCS})
    endif()
    if(ENABLE_SSE42)
        add_definitions(-DX86_SSE42_CRC_HASH)
        set(SSE42_SRCS ${ARCHDIR}/insert_string_sse.c)
        list(APPEND ZLIB_ARCH_SRCS ${SSE42_SRCS})
        add_definitions(-DX86_SSE42_CRC_INTRIN)
        add_definitions(-DX86_SSE42_CMP_STR)
        set(SSE42_SRCS ${ARCHDIR}/compare258_sse.c)
        list(APPEND ZLIB_ARCH_SRCS ${SSE42_SRCS})
    endif()
    if(ENABLE_SSSE3)
        add_definitions(-DX86_SSSE3 -DX86_SSSE3_ADLER32)
        set(SSSE3_SRCS ${ARCHDIR}/adler32_ssse3.c)
        list(APPEND ZLIB_ARCH_SRCS ${SSSE3_SRCS})
    endif()
    if(ENABLE_PCLMULQDQ)
        add_definitions(-DX86_PCLMULQDQ_CRC)
        set(PCLMULQDQ_SRCS ${ARCHDIR}/crc_folding.c)
        list(APPEND ZLIB_ARCH_SRCS ${PCLMULQDQ_SRCS})
    endif()

    add_definitions(-DX86_SSE2 -DX86_SSE2_CHUNKSET -DX86_SSE2_SLIDEHASH)
    set(SSE2_SRCS ${ARCHDIR}/chunkset_sse.c ${ARCHDIR}/slide_sse.c)
    list(APPEND ZLIB_ARCH_SRCS ${SSE2_SRCS})
    add_definitions(-DX86_NOCHECK_SSE2)
endif ()

macro(generate_cmakein input output)
    file(REMOVE ${output})
    file(STRINGS ${input} _lines)
    foreach(_line IN LISTS _lines)
        string(REGEX REPLACE "#ifdef HAVE_UNISTD_H.*" "@ZCONF_UNISTD_LINE@" _line "${_line}")
        string(REGEX REPLACE "#ifdef NEED_PTRDIFF_T.*" "@ZCONF_PTRDIFF_LINE@" _line "${_line}")
        if(NEED_PTRDIFF_T)
            string(REGEX REPLACE "typedef PTRDIFF_TYPE" "typedef @PTRDIFF_TYPE@" _line "${_line}")
        endif()
        file(APPEND ${output} "${_line}\n")
    endforeach()
endmacro(generate_cmakein)

generate_cmakein(${SOURCE_DIR}/zconf.h.in ${CMAKE_CURRENT_BINARY_DIR}/zconf.h.cmakein)

set(ZLIB_SRCS
    ${SOURCE_DIR}/adler32.c
    ${SOURCE_DIR}/chunkset.c
    ${SOURCE_DIR}/compare258.c
    ${SOURCE_DIR}/compress.c
    ${SOURCE_DIR}/crc32.c
    ${SOURCE_DIR}/crc32_comb.c
    ${SOURCE_DIR}/deflate.c
    ${SOURCE_DIR}/deflate_fast.c
    ${SOURCE_DIR}/deflate_medium.c
    ${SOURCE_DIR}/deflate_quick.c
    ${SOURCE_DIR}/deflate_slow.c
    ${SOURCE_DIR}/functable.c
    ${SOURCE_DIR}/infback.c
    ${SOURCE_DIR}/inffast.c
    ${SOURCE_DIR}/inflate.c
    ${SOURCE_DIR}/inftrees.c
    ${SOURCE_DIR}/insert_string.c
    ${SOURCE_DIR}/trees.c
    ${SOURCE_DIR}/uncompr.c
    ${SOURCE_DIR}/zutil.c
    ${SOURCE_DIR}/gzlib.c
    ${SOURCE_DIR}/gzread.c
    ${SOURCE_DIR}/gzwrite.c
)

set(ZLIB_ALL_SRCS ${ZLIB_SRCS} ${ZLIB_ARCH_SRCS})

add_library(_zlib ${ZLIB_ALL_SRCS})
add_library(ch_contrib::zlib ALIAS _zlib)

# https://github.com/zlib-ng/zlib-ng/pull/733
# This is disabed by default
add_compile_definitions(Z_TLS=__thread)

if(HAVE_UNISTD_H)
  SET(ZCONF_UNISTD_LINE "#if 1    /* was set to #if 1 by configure/cmake/etc */")
else()
  SET(ZCONF_UNISTD_LINE "#if 0    /* was set to #if 0 by configure/cmake/etc */")
endif()
if(NEED_PTRDIFF_T)
    SET(ZCONF_PTRDIFF_LINE "#if 1    /* was set to #if 1 by configure/cmake/etc */")
else()
    SET(ZCONF_PTRDIFF_LINE "#ifdef NEED_PTRDIFF_T    /* may be set to #if 1 by configure/cmake/etc */")
endif()

set(ZLIB_PC ${CMAKE_CURRENT_BINARY_DIR}/zlib.pc)
configure_file(${SOURCE_DIR}/zlib.pc.cmakein ${ZLIB_PC} @ONLY)
configure_file(${CMAKE_CURRENT_BINARY_DIR}/zconf.h.cmakein ${CMAKE_CURRENT_BINARY_DIR}/zconf.h @ONLY)

# We should use same defines when including zlib.h as used when zlib compiled
target_compile_definitions (_zlib PUBLIC ZLIB_COMPAT WITH_GZFILEOP)
if (ARCH_AMD64 OR ARCH_AARCH64)
    target_compile_definitions (_zlib PUBLIC X86_64 UNALIGNED_OK)
endif ()

target_include_directories(_zlib SYSTEM BEFORE PUBLIC ${SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
