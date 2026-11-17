option(ENABLE_ISAL_LIBRARY "Enable ISA-L library" ${ENABLE_LIBRARIES})

# ISA-L is only available for x86-64, so it shall be disabled for other platforms
if (NOT ARCH_AMD64)
    set (ENABLE_ISAL_LIBRARY OFF)
endif ()

if (NOT ENABLE_ISAL_LIBRARY)
    message(STATUS "Not using isa-l")
    return()
endif()

set(ISAL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/isa-l")

# The YASM and NASM assembers are somewhat mutually compatible. ISAL specifically needs NASM. If only YASM is installed, then check_language(ASM_NASM)
# below happily finds YASM, leading to weird errors at build time. Therefore, do an explicit check for NASM here.
find_program(NASM_PATH NAMES nasm)
if (NOT NASM_PATH)
    message(FATAL_ERROR "Please install NASM from 'https://www.nasm.us/' because NASM compiler can not be found!")
endif ()

include(CheckLanguage)
check_language(ASM_NASM)
if(NOT CMAKE_ASM_NASM_COMPILER)
    message(FATAL_ERROR "Please install NASM from 'https://www.nasm.us/' because NASM compiler can not be found!")
endif()

enable_language(ASM_NASM)

set(ISAL_C_SRC
        ${ISAL_SOURCE_DIR}/crc/crc_base_aliases.c
        ${ISAL_SOURCE_DIR}/crc/crc_base.c
        ${ISAL_SOURCE_DIR}/crc/crc64_base.c
        ${ISAL_SOURCE_DIR}/erasure_code/ec_base.c
        ${ISAL_SOURCE_DIR}/erasure_code/ec_base_aliases.c
        ${ISAL_SOURCE_DIR}/erasure_code/ec_highlevel_func.c
        ${ISAL_SOURCE_DIR}/igzip/adler32_base.c
        ${ISAL_SOURCE_DIR}/igzip/encode_df.c
        ${ISAL_SOURCE_DIR}/igzip/flatten_ll.c
        ${ISAL_SOURCE_DIR}/igzip/huff_codes.c
        ${ISAL_SOURCE_DIR}/igzip/hufftables_c.c
        ${ISAL_SOURCE_DIR}/igzip/igzip_base_aliases.c
        ${ISAL_SOURCE_DIR}/igzip/igzip_base.c
        ${ISAL_SOURCE_DIR}/igzip/igzip_icf_base.c
        ${ISAL_SOURCE_DIR}/igzip/igzip_icf_body.c
        ${ISAL_SOURCE_DIR}/igzip/igzip_inflate.c
        ${ISAL_SOURCE_DIR}/igzip/igzip.c
        ${ISAL_SOURCE_DIR}/mem/mem_zero_detect_base_aliases.c
        ${ISAL_SOURCE_DIR}/mem/mem_zero_detect_base.c
        ${ISAL_SOURCE_DIR}/raid/raid_base_aliases.c
        ${ISAL_SOURCE_DIR}/raid/raid_base.c
)

set(ISAL_ASM_SRC
        ${ISAL_SOURCE_DIR}/crc/crc_multibinary.asm
        ${ISAL_SOURCE_DIR}/crc/crc16_t10dif_01.asm
        ${ISAL_SOURCE_DIR}/crc/crc16_t10dif_02.asm
        ${ISAL_SOURCE_DIR}/crc/crc16_t10dif_by4.asm
        ${ISAL_SOURCE_DIR}/crc/crc16_t10dif_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc16_t10dif_copy_by4_02.asm
        ${ISAL_SOURCE_DIR}/crc/crc16_t10dif_copy_by4.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_gzip_refl_by8_02.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_gzip_refl_by8.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_gzip_refl_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_ieee_01.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_ieee_02.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_ieee_by4.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_ieee_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_iscsi_00.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_iscsi_01.asm
        ${ISAL_SOURCE_DIR}/crc/crc32_iscsi_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_ecma_norm_by8.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_ecma_norm_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_ecma_refl_by8.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_ecma_refl_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_iso_norm_by8.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_iso_norm_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_iso_refl_by8.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_iso_refl_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_jones_norm_by8.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_jones_norm_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_jones_refl_by8.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_jones_refl_by16_10.asm
        ${ISAL_SOURCE_DIR}/crc/crc64_multibinary.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_dot_prod_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_dot_prod_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_dot_prod_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_dot_prod_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/ec_multibinary.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_mad_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_mad_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_mad_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_2vect_mad_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_dot_prod_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_dot_prod_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_dot_prod_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_dot_prod_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_mad_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_mad_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_mad_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_3vect_mad_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_dot_prod_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_dot_prod_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_dot_prod_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_dot_prod_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_mad_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_mad_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_mad_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_4vect_mad_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_dot_prod_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_dot_prod_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_dot_prod_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_dot_prod_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_mad_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_mad_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_mad_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_5vect_mad_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_dot_prod_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_dot_prod_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_dot_prod_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_dot_prod_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_mad_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_mad_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_mad_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_6vect_mad_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_dot_prod_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_dot_prod_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_dot_prod_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_dot_prod_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_mad_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_mad_avx2.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_mad_avx512.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_mad_sse.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_mul_avx.asm
        ${ISAL_SOURCE_DIR}/erasure_code/gf_vect_mul_sse.asm
        ${ISAL_SOURCE_DIR}/igzip/adler32_avx2_4.asm
        ${ISAL_SOURCE_DIR}/igzip/adler32_sse.asm
        ${ISAL_SOURCE_DIR}/igzip/bitbuf2.asm
        ${ISAL_SOURCE_DIR}/igzip/encode_df_04.asm
        ${ISAL_SOURCE_DIR}/igzip/encode_df_06.asm
        ${ISAL_SOURCE_DIR}/igzip/heap_macros.asm
        ${ISAL_SOURCE_DIR}/igzip/huffman.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_body.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_compare_types.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_decode_block_stateless_01.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_decode_block_stateless_04.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_deflate_hash.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_finish.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_gen_icf_map_lh1_04.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_gen_icf_map_lh1_06.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_icf_body_h1_gr_bt.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_icf_finish.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_inflate_multibinary.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_multibinary.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_set_long_icf_fg_04.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_set_long_icf_fg_06.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_update_histogram_01.asm
        ${ISAL_SOURCE_DIR}/igzip/igzip_update_histogram_04.asm
        ${ISAL_SOURCE_DIR}/igzip/lz0a_const.asm
        ${ISAL_SOURCE_DIR}/igzip/options.asm
        ${ISAL_SOURCE_DIR}/igzip/proc_heap.asm
        ${ISAL_SOURCE_DIR}/igzip/rfc1951_lookup.asm
        ${ISAL_SOURCE_DIR}/igzip/stdmac.asm
        ${ISAL_SOURCE_DIR}/mem/mem_multibinary.asm
        ${ISAL_SOURCE_DIR}/mem/mem_zero_detect_avx.asm
        ${ISAL_SOURCE_DIR}/mem/mem_zero_detect_avx2.asm
        ${ISAL_SOURCE_DIR}/mem/mem_zero_detect_avx512.asm
        ${ISAL_SOURCE_DIR}/mem/mem_zero_detect_sse.asm
        ${ISAL_SOURCE_DIR}/raid/pq_check_sse.asm
        ${ISAL_SOURCE_DIR}/raid/pq_gen_avx.asm
        ${ISAL_SOURCE_DIR}/raid/pq_gen_avx2.asm
        ${ISAL_SOURCE_DIR}/raid/pq_gen_avx512.asm
        ${ISAL_SOURCE_DIR}/raid/pq_gen_sse.asm
        ${ISAL_SOURCE_DIR}/raid/raid_multibinary.asm
        ${ISAL_SOURCE_DIR}/raid/xor_check_sse.asm
        ${ISAL_SOURCE_DIR}/raid/xor_gen_avx.asm
        ${ISAL_SOURCE_DIR}/raid/xor_gen_avx512.asm
        ${ISAL_SOURCE_DIR}/raid/xor_gen_sse.asm
)

# Adding ISA-L library target
add_library(_isal ${ISAL_C_SRC} ${ISAL_ASM_SRC})

# Setting external and internal interfaces for ISA-L library
target_include_directories(_isal
        PUBLIC ${ISAL_SOURCE_DIR}/include
        PUBLIC ${ISAL_SOURCE_DIR}/igzip
        PUBLIC ${ISAL_SOURCE_DIR}/crc
        PUBLIC ${ISAL_SOURCE_DIR}/erasure_code)

# Here must remove "-fno-sanitize=undefined" from COMPILE_OPTIONS.
# Otherwise nasm compiler would fail to proceed due to unrecognition of "-fno-sanitize=undefined"
if (SANITIZE STREQUAL "undefined")
    get_target_property(target_options _isal COMPILE_OPTIONS)
    list(REMOVE_ITEM target_options "-fno-sanitize=undefined")
    set_property(TARGET _isal PROPERTY COMPILE_OPTIONS ${target_options})
endif()

add_library(ch_contrib::isal ALIAS _isal)
