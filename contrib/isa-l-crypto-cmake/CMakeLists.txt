option(ENABLE_ISAL_CRYPTO "Enable ISA-L Crypto library" ${ENABLE_LIBRARIES})

if (NOT OS_LINUX OR NOT (ARCH_AMD64 OR ARCH_AARCH64))
    set(ENABLE_ISAL_CRYPTO OFF)
endif ()

if (NOT ENABLE_ISAL_CRYPTO)
    message(STATUS "Not using isa-l_crypto")
    return()
endif ()

set(ISAL_CRYPTO_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/isa-l_crypto")

set(ISAL_CRYPTO_MD5_SOURCES
    ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb.c
)

if (ARCH_AMD64)
    find_program(NASM_PATH NAMES nasm)
    if (NOT NASM_PATH)
        message(FATAL_ERROR "Please install NASM from 'https://www.nasm.us/' because NASM compiler can not be found!")
    endif ()

    set(CMAKE_ASM_NASM_COMPILER "${NASM_PATH}")
    set(CMAKE_ASM_NASM_COMPILER_WORKS TRUE)
    set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -f elf64 -D LINUX")
    enable_language(ASM_NASM)

    list(APPEND ISAL_CRYPTO_MD5_SOURCES
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_ctx_sse.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_ctx_avx.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_ctx_avx2.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_ctx_base.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_init_sse.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_init_avx2.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_init_avx512.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_submit_sse.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_submit_avx.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_submit_avx2.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_flush_sse.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_flush_avx.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_flush_avx2.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_x4x2_sse.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_x4x2_avx.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_x8x2_avx2.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_multibinary.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_submit_avx512.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_mgr_flush_avx512.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_mb_x16x2_avx512.asm
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_ctx_avx512.c
    )
elseif (ARCH_AARCH64)
    enable_language(ASM)
    set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -D__ASSEMBLY__")

    list(APPEND ISAL_CRYPTO_MD5_SOURCES
        ${CMAKE_CURRENT_LIST_DIR}/md5_aarch64_asimd_dispatch.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/md5_ctx_base.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/aarch64/md5_ctx_aarch64_asimd.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/aarch64/md5_mb_mgr_aarch64_asimd.c
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/aarch64/md5_mb_asimd_x4.S
        ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb/aarch64/md5_mb_asimd_x1.S
    )
endif ()

add_library(_isal_crypto ${ISAL_CRYPTO_MD5_SOURCES})
add_library(ch_contrib::isal_crypto ALIAS _isal_crypto)

target_compile_definitions(_isal_crypto PRIVATE NO_COMPAT_ISAL_CRYPTO_API_2_24)
target_compile_options(_isal_crypto PRIVATE "$<$<COMPILE_LANGUAGE:C>:-w>")

target_include_directories(_isal_crypto SYSTEM PUBLIC
    ${ISAL_CRYPTO_SOURCE_DIR}/include
)

target_include_directories(_isal_crypto SYSTEM PRIVATE
    ${ISAL_CRYPTO_SOURCE_DIR}/include/isa-l_crypto
    ${ISAL_CRYPTO_SOURCE_DIR}/include/internal
    ${ISAL_CRYPTO_SOURCE_DIR}/md5_mb
)

if (ARCH_AMD64)
    foreach(source IN LISTS ISAL_CRYPTO_MD5_SOURCES)
        if (source MATCHES "\\.asm$")
            get_filename_component(source_dir ${source} DIRECTORY)
            set_source_files_properties(${source} PROPERTIES
                INCLUDE_DIRECTORIES "${ISAL_CRYPTO_SOURCE_DIR};${ISAL_CRYPTO_SOURCE_DIR}/include;${ISAL_CRYPTO_SOURCE_DIR}/md5_mb;${source_dir}")
        endif ()
    endforeach()
endif ()

if (SANITIZE STREQUAL "undefined" OR SANITIZE STREQUAL "address,undefined")
    get_target_property(target_options _isal_crypto COMPILE_OPTIONS)
    list(REMOVE_ITEM target_options "-fno-sanitize=undefined")
    set_property(TARGET _isal_crypto PROPERTY COMPILE_OPTIONS ${target_options})
endif ()
