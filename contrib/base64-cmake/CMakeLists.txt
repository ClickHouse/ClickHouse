SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/base64)

add_library(base64_scalar OBJECT ${LIBRARY_DIR}/turbob64c.c ${LIBRARY_DIR}/turbob64d.c)
add_library(base64_ssse3 OBJECT ${LIBRARY_DIR}/turbob64sse.c) # This file also contains code for ARM NEON

if (ARCH_AMD64)
    add_library(base64_avx OBJECT ${LIBRARY_DIR}/turbob64sse.c) # This is not a mistake. One file is compiled twice.
    add_library(base64_avx2 OBJECT ${LIBRARY_DIR}/turbob64avx2.c)
endif ()

target_compile_options(base64_scalar PRIVATE -falign-loops)

if (ARCH_AMD64)
    target_compile_options(base64_ssse3 PRIVATE -mssse3 -falign-loops)
    target_compile_options(base64_avx PRIVATE -falign-loops -mavx)
    target_compile_options(base64_avx2 PRIVATE -falign-loops -mavx2)
else ()
    target_compile_options(base64_ssse3 PRIVATE -falign-loops)
endif ()

if (ARCH_AMD64)
    add_library(base64
        $<TARGET_OBJECTS:base64_scalar>
        $<TARGET_OBJECTS:base64_ssse3>
        $<TARGET_OBJECTS:base64_avx>
        $<TARGET_OBJECTS:base64_avx2>)
else ()
    add_library(base64
        $<TARGET_OBJECTS:base64_scalar>
        $<TARGET_OBJECTS:base64_ssse3>)
endif ()

target_include_directories(base64 PUBLIC ${LIBRARY_DIR})
