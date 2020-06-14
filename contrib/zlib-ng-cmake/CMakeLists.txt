set(ZLIB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng")
set(ZLIB_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng")
set(ZLIB_SRC
    ${ZLIB_SRC_DIR}/adler32.c
    ${ZLIB_SRC_DIR}/compare258.c
    ${ZLIB_SRC_DIR}/compress.c
    ${ZLIB_SRC_DIR}/crc32.c
    ${ZLIB_SRC_DIR}/deflate.c
    ${ZLIB_SRC_DIR}/deflate_quick.c
    ${ZLIB_SRC_DIR}/deflate_fast.c
    ${ZLIB_SRC_DIR}/deflate_medium.c
    ${ZLIB_SRC_DIR}/deflate_slow.c
    ${ZLIB_SRC_DIR}/functable.c
    ${ZLIB_SRC_DIR}/inflate.c
    ${ZLIB_SRC_DIR}/infback.c
    ${ZLIB_SRC_DIR}/inftrees.c
    ${ZLIB_SRC_DIR}/inffast.c
    ${ZLIB_SRC_DIR}/insert_string.c
    ${ZLIB_SRC_DIR}/trees.c
    ${ZLIB_SRC_DIR}/uncompr.c
    ${ZLIB_SRC_DIR}/zutil.c
    ${ZLIB_SRC_DIR}/gzclose.c
    ${ZLIB_SRC_DIR}/gzlib.c
    ${ZLIB_SRC_DIR}/gzread.c
    ${ZLIB_SRC_DIR}/gzwrite.c
)

if (ARCH_AMD64)
    set(ZLIB_SRC ${ZLIB_SRC}
        ${ZLIB_SRC_DIR}/arch/x86/x86.c
        ${ZLIB_SRC_DIR}/arch/x86/crc_folding.c
        ${ZLIB_SRC_DIR}/arch/x86/adler32_ssse3.c
        ${ZLIB_SRC_DIR}/arch/x86/compare258_sse.c
        ${ZLIB_SRC_DIR}/arch/x86/insert_string_sse.c
        ${ZLIB_SRC_DIR}/arch/x86/slide_sse.c)
elseif (ARCH_AARCH64)
    set(ZLIB_SRC ${ZLIB_SRC}
        ${ZLIB_SRC_DIR}/arch/arm/armfeature.c
        ${ZLIB_SRC_DIR}/arch/arm/adler32_neon.c
        ${ZLIB_SRC_DIR}/arch/arm/slide_neon.c
        ${ZLIB_SRC_DIR}/arch/arm/crc32_acle.c
        ${ZLIB_SRC_DIR}/arch/arm/insert_string_acle.c)
endif ()

add_library(zlib ${ZLIB_SRC})
target_include_directories(zlib SYSTEM PUBLIC "${ZLIB_INCLUDE_DIR}")
target_include_directories(zlib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")   # zconf.h

target_compile_definitions(zlib PRIVATE
    -DHAVE_BUILTIN_CTZL -DHAVE_VISIBILITY_HIDDEN -DHAVE_VISIBILITY_INTERNAL
    -DWITH_GZFILEOP
    -DZLIB_COMPAT -D_LARGEFILE64_SOURCE=1 -D__USE_LARGEFILE64)

if (ARCH_AMD64)
    target_compile_definitions(zlib PRIVATE
        -DX86_64 -DX86_CPUID -DX86_PCLMULQDQ_CRC -DX86_QUICK_STRATEGY -DX86_SSE2 -DX86_SSE42_CRC_HASH -DX86_SSE42_CRC_INTRIN)
    target_compile_options(zlib PRIVATE -mpclmul)
elseif (ARCH_AARCH64)
    target_compile_definitions(zlib PRIVATE
        -DARM_CPUID -DARM_NEON_ADLER32 -DARM_NEON_SLIDEHASH -DARM_ACLE_CRC_HASH)
endif ()
