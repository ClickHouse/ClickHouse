## The Intel® QPL provides high performance implementations of data processing functions for existing hardware accelerator, and/or software path in case if hardware accelerator is not available.
set (UUID_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl-cmake")
set (QPL_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl")
set (QPL_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl/sources")
set (QPL_BINARY_DIR "${ClickHouse_BINARY_DIR}/build/contrib/qpl")
set (EFFICIENT_WAIT OFF)
set (LOG_HW_INIT OFF)
set (SANITIZE_MEMORY OFF)
set (SANITIZE_THREADS OFF)
set (LIB_FUZZING_ENGINE OFF)
set (DYNAMIC_LOADING_LIBACCEL_CONFIG OFF)

function(GetLibraryVersion _content _outputVar)
    string(REGEX MATCHALL "QPL VERSION (.+) LANGUAGES" VERSION_REGEX "${_content}")
    SET(${_outputVar} ${CMAKE_MATCH_1} PARENT_SCOPE)
endfunction()

set (QPL_VERSION 1.6.0)

message(STATUS "Intel QPL version: ${QPL_VERSION}")

# There are 5 source subdirectories under $QPL_SRC_DIR: c_api, core-iaa, core-sw, middle-layer and isal.
# Generate 8 library targets: qpl_c_api, core_iaa, qplcore_px, qplcore_avx512, qplcore_sw_dispatcher, middle_layer_lib, isal and isal_asm,
# which are then combined into static or shared qpl.
# Output ch_contrib::qpl by linking with 8 library targets.

# Note, QPL has integrated a customized version of ISA-L to meet specific needs.
# This version has been significantly modified and there are no plans to maintain compatibility with the upstream version
# or upgrade the current copy.

## cmake/CompileOptions.cmake and automatic wrappers generation

# ==========================================================================
# Copyright (C) 2022 Intel Corporation
#
# SPDX-License-Identifier: MIT
# ==========================================================================

set(QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS "-fno-exceptions;-fno-rtti")

function(modify_standard_language_flag)
    # Declaring function parameters
    set(OPTIONS "")
    set(ONE_VALUE_ARGS
        LANGUAGE_NAME
        FLAG_NAME
        NEW_FLAG_VALUE)
    set(MULTI_VALUE_ARGS "")

    # Parsing function parameters
    cmake_parse_arguments(MODIFY
                          "${OPTIONS}"
                          "${ONE_VALUE_ARGS}"
                          "${MULTI_VALUE_ARGS}"
                          ${ARGN})

    # Variables
    set(FLAG_REGULAR_EXPRESSION "${MODIFY_FLAG_NAME}.*[ ]*")
    set(NEW_VALUE "${MODIFY_FLAG_NAME}${MODIFY_NEW_FLAG_VALUE}")

    # Replacing specified flag with new value
    string(REGEX REPLACE
           ${FLAG_REGULAR_EXPRESSION} ${NEW_VALUE}
           NEW_COMPILE_FLAGS
           "${CMAKE_${MODIFY_LANGUAGE_NAME}_FLAGS}")

    # Returning the value
    set(CMAKE_${MODIFY_LANGUAGE_NAME}_FLAGS ${NEW_COMPILE_FLAGS} PARENT_SCOPE)
endfunction()

function(get_function_name_with_default_bit_width in_function_name bit_width out_function_name)

    if(in_function_name MATCHES ".*_i")

        string(REPLACE "_i" "" in_function_name ${in_function_name})

        set(${out_function_name} "${in_function_name}_${bit_width}_i" PARENT_SCOPE)

    else()

        set(${out_function_name} "${in_function_name}_${bit_width}" PARENT_SCOPE)

    endif()

endfunction()

macro(get_list_of_supported_optimizations PLATFORMS_LIST)
    list(APPEND PLATFORMS_LIST "")
    list(APPEND PLATFORMS_LIST "px")
    list(APPEND PLATFORMS_LIST "avx512")
endmacro(get_list_of_supported_optimizations)

function(generate_unpack_kernel_arrays current_directory PLATFORMS_LIST)
    list(APPEND UNPACK_POSTFIX_LIST "")
    list(APPEND UNPACK_PRLE_POSTFIX_LIST "")
    list(APPEND PACK_POSTFIX_LIST "")
    list(APPEND PACK_INDEX_POSTFIX_LIST "")
    list(APPEND SCAN_POSTFIX_LIST "")
    list(APPEND DEFAULT_BIT_WIDTH_FUNCTIONS_LIST "")
    list(APPEND DEFAULT_BIT_WIDTH_LIST "")

    #create list of functions that use only 8u 16u 32u postfixes
    list(APPEND DEFAULT_BIT_WIDTH_FUNCTIONS_LIST "unpack_prle")
    list(APPEND DEFAULT_BIT_WIDTH_FUNCTIONS_LIST "extract")
    list(APPEND DEFAULT_BIT_WIDTH_FUNCTIONS_LIST "extract_i")
    list(APPEND DEFAULT_BIT_WIDTH_FUNCTIONS_LIST "select")
    list(APPEND DEFAULT_BIT_WIDTH_FUNCTIONS_LIST "select_i")
    list(APPEND DEFAULT_BIT_WIDTH_FUNCTIONS_LIST "expand")

    #create default bit width list
    list(APPEND DEFAULT_BIT_WIDTH_LIST "8u")
    list(APPEND DEFAULT_BIT_WIDTH_LIST "16u")
    list(APPEND DEFAULT_BIT_WIDTH_LIST "32u")

    #create scan kernel postfixes
    list(APPEND SCAN_COMPARATOR_LIST "")

    list(APPEND SCAN_COMPARATOR_LIST "eq")
    list(APPEND SCAN_COMPARATOR_LIST "ne")
    list(APPEND SCAN_COMPARATOR_LIST "lt")
    list(APPEND SCAN_COMPARATOR_LIST "le")
    list(APPEND SCAN_COMPARATOR_LIST "gt")
    list(APPEND SCAN_COMPARATOR_LIST "ge")
    list(APPEND SCAN_COMPARATOR_LIST "range")
    list(APPEND SCAN_COMPARATOR_LIST "not_range")

    foreach(SCAN_COMPARATOR IN LISTS SCAN_COMPARATOR_LIST)
        list(APPEND SCAN_POSTFIX_LIST "_${SCAN_COMPARATOR}_8u")
        list(APPEND SCAN_POSTFIX_LIST "_${SCAN_COMPARATOR}_16u8u")
        list(APPEND SCAN_POSTFIX_LIST "_${SCAN_COMPARATOR}_32u8u")
    endforeach()

    # create unpack kernel postfixes
    foreach(input_width RANGE 1 32 1)
        if(input_width LESS 8 OR input_width EQUAL 8)
            list(APPEND UNPACK_POSTFIX_LIST "_${input_width}u8u")

        elseif(input_width LESS 16 OR input_width EQUAL 16)
            list(APPEND UNPACK_POSTFIX_LIST "_${input_width}u16u")

        else()
            list(APPEND UNPACK_POSTFIX_LIST "_${input_width}u32u")
        endif()
    endforeach()

    # create pack kernel postfixes
    foreach(output_width RANGE 1 8 1)
        list(APPEND PACK_POSTFIX_LIST "_8u${output_width}u")
    endforeach()

    foreach(output_width RANGE 9 16 1)
        list(APPEND PACK_POSTFIX_LIST "_16u${output_width}u")
    endforeach()

    foreach(output_width RANGE 17 32 1)
        list(APPEND PACK_POSTFIX_LIST "_32u${output_width}u")
    endforeach()

    list(APPEND PACK_POSTFIX_LIST "_8u16u")
    list(APPEND PACK_POSTFIX_LIST "_8u32u")
    list(APPEND PACK_POSTFIX_LIST "_16u32u")

    # create pack index kernel postfixes
    list(APPEND PACK_INDEX_POSTFIX_LIST "_nu")
    list(APPEND PACK_INDEX_POSTFIX_LIST "_8u")
    list(APPEND PACK_INDEX_POSTFIX_LIST "_8u16u")
    list(APPEND PACK_INDEX_POSTFIX_LIST "_8u32u")

    # write to file
    file(MAKE_DIRECTORY ${current_directory}/generated)

    foreach(PLATFORM_VALUE IN LISTS PLATFORMS_LIST)
        set(directory "${current_directory}/generated")
        set(PLATFORM_PREFIX "${PLATFORM_VALUE}_")

        #
        # Write unpack table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}unpack.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}unpack.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}unpack.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}unpack.cpp "unpack_table_t ${PLATFORM_PREFIX}unpack_table = {\n")

        #write LE kernels
        foreach(UNPACK_POSTFIX IN LISTS UNPACK_POSTFIX_LIST)
            file(APPEND ${directory}/${PLATFORM_PREFIX}unpack.cpp "\t${PLATFORM_PREFIX}qplc_unpack${UNPACK_POSTFIX},\n")
        endforeach()

        #write BE kernels

        #get last element of the list
        set(LAST_ELEMENT "")
        list(GET UNPACK_POSTFIX_LIST -1 LAST_ELEMENT)

        foreach(UNPACK_POSTFIX IN LISTS UNPACK_POSTFIX_LIST)

            if(UNPACK_POSTFIX STREQUAL LAST_ELEMENT)
                file(APPEND ${directory}/${PLATFORM_PREFIX}unpack.cpp "\t${PLATFORM_PREFIX}qplc_unpack_be${UNPACK_POSTFIX}};\n")
            else()
                file(APPEND ${directory}/${PLATFORM_PREFIX}unpack.cpp "\t${PLATFORM_PREFIX}qplc_unpack_be${UNPACK_POSTFIX},\n")
            endif()
        endforeach()

        file(APPEND ${directory}/${PLATFORM_PREFIX}unpack.cpp "}\n")

        #
        # Write pack table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}pack.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack.cpp "pack_table_t ${PLATFORM_PREFIX}pack_table = {\n")

        #write LE kernels
        foreach(PACK_POSTFIX IN LISTS PACK_POSTFIX_LIST)
            file(APPEND ${directory}/${PLATFORM_PREFIX}pack.cpp "\t${PLATFORM_PREFIX}qplc_pack${PACK_POSTFIX},\n")
        endforeach()

        #write BE kernels

        #get last element of the list
        set(LAST_ELEMENT "")
        list(GET PACK_POSTFIX_LIST -1 LAST_ELEMENT)

        foreach(PACK_POSTFIX IN LISTS PACK_POSTFIX_LIST)

            if(PACK_POSTFIX STREQUAL LAST_ELEMENT)
                file(APPEND ${directory}/${PLATFORM_PREFIX}pack.cpp "\t${PLATFORM_PREFIX}qplc_pack_be${PACK_POSTFIX}};\n")
            else()
                file(APPEND ${directory}/${PLATFORM_PREFIX}pack.cpp "\t${PLATFORM_PREFIX}qplc_pack_be${PACK_POSTFIX},\n")
            endif()
        endforeach()

        file(APPEND ${directory}/${PLATFORM_PREFIX}pack.cpp "}\n")

        #
        # Write scan table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}scan.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}scan.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}scan.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}scan.cpp "scan_table_t ${PLATFORM_PREFIX}scan_table = {\n")

        #get last element of the list
        set(LAST_ELEMENT "")
        list(GET SCAN_POSTFIX_LIST -1 LAST_ELEMENT)

        foreach(SCAN_POSTFIX IN LISTS SCAN_POSTFIX_LIST)

            if(SCAN_POSTFIX STREQUAL LAST_ELEMENT)
                file(APPEND ${directory}/${PLATFORM_PREFIX}scan.cpp "\t${PLATFORM_PREFIX}qplc_scan${SCAN_POSTFIX}};\n")
            else()
                file(APPEND ${directory}/${PLATFORM_PREFIX}scan.cpp "\t${PLATFORM_PREFIX}qplc_scan${SCAN_POSTFIX},\n")
            endif()
        endforeach()

        file(APPEND ${directory}/${PLATFORM_PREFIX}scan.cpp "}\n")

        #
        # Write scan_i table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}scan_i.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}scan_i.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}scan_i.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}scan_i.cpp "scan_i_table_t ${PLATFORM_PREFIX}scan_i_table = {\n")

        #get last element of the list
        set(LAST_ELEMENT "")
        list(GET SCAN_POSTFIX_LIST -1 LAST_ELEMENT)

        foreach(SCAN_POSTFIX IN LISTS SCAN_POSTFIX_LIST)

            if(SCAN_POSTFIX STREQUAL LAST_ELEMENT)
                file(APPEND ${directory}/${PLATFORM_PREFIX}scan_i.cpp "\t${PLATFORM_PREFIX}qplc_scan${SCAN_POSTFIX}_i};\n")
            else()
                file(APPEND ${directory}/${PLATFORM_PREFIX}scan_i.cpp "\t${PLATFORM_PREFIX}qplc_scan${SCAN_POSTFIX}_i,\n")
            endif()
        endforeach()

        file(APPEND ${directory}/${PLATFORM_PREFIX}scan_i.cpp "}\n")

        #
        # Write pack_index table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}pack_index.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "pack_index_table_t ${PLATFORM_PREFIX}pack_index_table = {\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_bits_nu,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_index_8u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_index_8u16u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_index_8u32u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_bits_be_nu,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_index_8u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_index_be_8u16u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "\t${PLATFORM_PREFIX}qplc_pack_index_be_8u32u};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}pack_index.cpp "}\n")

        #
        # Write default bit width functions
        #
        foreach(DEAULT_BIT_WIDTH_FUNCTION IN LISTS DEFAULT_BIT_WIDTH_FUNCTIONS_LIST)
            file(WRITE ${directory}/${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}.cpp "#include \"qplc_api.h\"\n")
            file(APPEND ${directory}/${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
            file(APPEND ${directory}/${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}.cpp "namespace qpl::core_sw::dispatcher\n{\n")
            file(APPEND ${directory}/${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}.cpp "${DEAULT_BIT_WIDTH_FUNCTION}_table_t ${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}_table = {\n")

            #get last element of the list
            set(LAST_ELEMENT "")
            list(GET DEFAULT_BIT_WIDTH_LIST -1 LAST_ELEMENT)

            foreach(BIT_WIDTH IN LISTS DEFAULT_BIT_WIDTH_LIST)

                set(FUNCTION_NAME "")
                get_function_name_with_default_bit_width(${DEAULT_BIT_WIDTH_FUNCTION} ${BIT_WIDTH} FUNCTION_NAME)

                if(BIT_WIDTH STREQUAL LAST_ELEMENT)
                    file(APPEND ${directory}/${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}.cpp "\t${PLATFORM_PREFIX}qplc_${FUNCTION_NAME}};\n")
                else()
                 file(APPEND ${directory}/${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}.cpp "\t${PLATFORM_PREFIX}qplc_${FUNCTION_NAME},\n")
                 endif()
            endforeach()

            file(APPEND ${directory}/${PLATFORM_PREFIX}${DEAULT_BIT_WIDTH_FUNCTION}.cpp "}\n")
        endforeach()

        #
        # Write aggregates table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}aggregates.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "aggregates_table_t ${PLATFORM_PREFIX}aggregates_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "\t${PLATFORM_PREFIX}qplc_bit_aggregates_8u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "\t${PLATFORM_PREFIX}qplc_aggregates_8u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "\t${PLATFORM_PREFIX}qplc_aggregates_16u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "\t${PLATFORM_PREFIX}qplc_aggregates_32u};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}aggregates.cpp "}\n")

        #
        # Write mem_copy functions table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "memory_copy_table_t ${PLATFORM_PREFIX}memory_copy_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "\t${PLATFORM_PREFIX}qplc_copy_8u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "\t${PLATFORM_PREFIX}qplc_copy_16u,\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "\t${PLATFORM_PREFIX}qplc_copy_32u};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}memory_copy.cpp "}\n")

        #
        # Write mem_copy functions table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}zero.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}zero.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}zero.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}zero.cpp "zero_table_t ${PLATFORM_PREFIX}zero_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}zero.cpp "\t${PLATFORM_PREFIX}qplc_zero_8u};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}zero.cpp "}\n")

        #
        # Write move functions table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}move.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}move.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}move.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}move.cpp "move_table_t ${PLATFORM_PREFIX}move_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}move.cpp "\t${PLATFORM_PREFIX}qplc_move_8u};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}move.cpp "}\n")

        #
        # Write crc64 function table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}crc64.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}crc64.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}crc64.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}crc64.cpp "crc64_table_t ${PLATFORM_PREFIX}crc64_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}crc64.cpp "\t${PLATFORM_PREFIX}qplc_crc64};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}crc64.cpp "}\n")

        #
        # Write xor_checksum function table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}xor_checksum.cpp "#include \"qplc_api.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}xor_checksum.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}xor_checksum.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}xor_checksum.cpp "xor_checksum_table_t ${PLATFORM_PREFIX}xor_checksum_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}xor_checksum.cpp "\t${PLATFORM_PREFIX}qplc_xor_checksum_8u};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}xor_checksum.cpp "}\n")

        #
        # Write deflate functions table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}deflate.cpp "#include \"deflate_slow_icf.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "#include \"deflate_hash_table.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "#include \"deflate_histogram.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "deflate_table_t ${PLATFORM_PREFIX}deflate_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "\t reinterpret_cast<void *>(&${PLATFORM_PREFIX}slow_deflate_icf_body),\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "\t reinterpret_cast<void *>(&${PLATFORM_PREFIX}deflate_histogram_reset),\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "\t reinterpret_cast<void *>(&${PLATFORM_PREFIX}deflate_hash_table_reset)};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate.cpp "}\n")

        #
        # Write deflate fix functions table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}deflate_fix.cpp "#include \"deflate_slow.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate_fix.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate_fix.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate_fix.cpp "deflate_fix_table_t ${PLATFORM_PREFIX}deflate_fix_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate_fix.cpp "\t reinterpret_cast<void *>(&${PLATFORM_PREFIX}slow_deflate_body)};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}deflate_fix.cpp "}\n")

        #
        # Write setup_dictionary functions table
        #
        file(WRITE ${directory}/${PLATFORM_PREFIX}setup_dictionary.cpp "#include \"deflate_slow_utils.h\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}setup_dictionary.cpp "#include \"dispatcher/dispatcher.hpp\"\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}setup_dictionary.cpp "namespace qpl::core_sw::dispatcher\n{\n")
        file(APPEND ${directory}/${PLATFORM_PREFIX}setup_dictionary.cpp "setup_dictionary_table_t ${PLATFORM_PREFIX}setup_dictionary_table = {\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}setup_dictionary.cpp "\t reinterpret_cast<void *>(&${PLATFORM_PREFIX}setup_dictionary)};\n")

        file(APPEND ${directory}/${PLATFORM_PREFIX}setup_dictionary.cpp "}\n")

    endforeach()
endfunction()

# [SUBDIR]isal

enable_language(ASM_NASM)

set(ISAL_C_SRC ${QPL_SRC_DIR}/isal/igzip/adler32_base.c
               ${QPL_SRC_DIR}/isal/igzip/huff_codes.c
               ${QPL_SRC_DIR}/isal/igzip/hufftables_c.c
               ${QPL_SRC_DIR}/isal/igzip/igzip.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_base.c
               ${QPL_SRC_DIR}/isal/igzip/flatten_ll.c
               ${QPL_SRC_DIR}/isal/igzip/encode_df.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_icf_base.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_inflate.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_icf_body.c
               ${QPL_SRC_DIR}/isal/crc/crc_base.c
               ${QPL_SRC_DIR}/isal/crc/crc64_base.c)

set(ISAL_ASM_SRC ${QPL_SRC_DIR}/isal/igzip/igzip_body.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_gen_icf_map_lh1_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_gen_icf_map_lh1_06.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_decode_block_stateless_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_finish.asm
                 ${QPL_SRC_DIR}/isal/igzip/encode_df_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/encode_df_06.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_decode_block_stateless_01.asm
                 ${QPL_SRC_DIR}/isal/igzip/proc_heap.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_icf_body_h1_gr_bt.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_icf_finish.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_inflate_multibinary.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_update_histogram_01.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_update_histogram_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/rfc1951_lookup.asm
                 ${QPL_SRC_DIR}/isal/igzip/adler32_sse.asm
                 ${QPL_SRC_DIR}/isal/igzip/adler32_avx2_4.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_deflate_hash.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_set_long_icf_fg_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_set_long_icf_fg_06.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_multibinary.asm
                 ${QPL_SRC_DIR}/isal/crc/crc_multibinary.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_gzip_refl_by8.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_gzip_refl_by8_02.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_gzip_refl_by16_10.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_01.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_02.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_by4.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_by16_10.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_iscsi_00.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_iscsi_01.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_iscsi_by16_10.asm)

# Adding ISA-L library target
add_library(isal OBJECT ${ISAL_C_SRC})
add_library(isal_asm OBJECT ${ISAL_ASM_SRC})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:isal>)

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:isal_asm>)

# Setting external and internal interfaces for ISA-L library
target_include_directories(isal
                        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/isal/include>
                        PUBLIC ${QPL_SRC_DIR}/isal/igzip)

set_target_properties(isal PROPERTIES
                        CXX_STANDARD 11
                        C_STANDARD 99)

# AS_FEATURE_LEVEL=10 means "Check SIMD capabilities of the target system at runtime and use up to AVX512 if available".
# HAVE_KNOWS_AVX512 means rely on AVX512 being available on the target system.
target_compile_options(isal_asm PRIVATE "-I${QPL_SRC_DIR}/isal/include/"
                       PRIVATE "-I${QPL_SRC_DIR}/isal/igzip/"
                       PRIVATE "-I${QPL_SRC_DIR}/isal/crc/"
                       PRIVATE "-DHAVE_AS_KNOWS_AVX512"
                       PRIVATE "-DAS_FEATURE_LEVEL=10"
                       PRIVATE "-DQPL_LIB")

# Here must remove "-fno-sanitize=undefined" from COMPILE_OPTIONS.
# Otherwise nasm compiler would fail to proceed due to unrecognition of "-fno-sanitize=undefined"
if (SANITIZE STREQUAL "undefined")
    get_target_property(target_options isal_asm COMPILE_OPTIONS)
    list(REMOVE_ITEM target_options "-fno-sanitize=undefined")
    set_property(TARGET isal_asm PROPERTY COMPILE_OPTIONS ${target_options})
endif()

target_compile_definitions(isal PUBLIC
                           QPL_LIB
                           NDEBUG)

# [SUBDIR]core-sw
# Create set of libraries corresponding to supported platforms for SW fallback which are implemented by AVX512 and non-AVX512 instructions respectively.
# The upper level QPL API will check SIMD capabilities of the target system at runtime and decide to call AVX512 function or non-AVX512 function.
# Hence, here we don't need put ENABLE_AVX512 CMake switch.

get_list_of_supported_optimizations(PLATFORMS_LIST)

foreach(PLATFORM_ID IN LISTS PLATFORMS_LIST)
    # Find Core Sources
    file(GLOB SOURCES
        ${QPL_SRC_DIR}/core-sw/src/checksums/*.c
        ${QPL_SRC_DIR}/core-sw/src/filtering/*.c
        ${QPL_SRC_DIR}/core-sw/src/other/*.c
        ${QPL_SRC_DIR}/core-sw/src/compression/*.c)

    file(GLOB DATA_SOURCES
        ${QPL_SRC_DIR}/core-sw/src/data/*.c)

    # Create library
    add_library(qplcore_${PLATFORM_ID} OBJECT ${SOURCES})

    set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
                $<TARGET_OBJECTS:qplcore_${PLATFORM_ID}>)

    target_include_directories(qplcore_${PLATFORM_ID}
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw>
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/include>
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/include>
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/compression/include>
                                PRIVATE $<TARGET_PROPERTY:isal,INTERFACE_INCLUDE_DIRECTORIES>)

        # Set specific compiler options and/or definitions based on a platform
    if (${PLATFORM_ID} MATCHES "avx512")
        target_compile_definitions(qplcore_${PLATFORM_ID} PRIVATE PLATFORM=2)
        target_compile_options(qplcore_${PLATFORM_ID} PRIVATE -march=skylake-avx512)
    else() # Create default px library
        target_compile_definitions(qplcore_${PLATFORM_ID} PRIVATE PLATFORM=0)
    endif()

    target_link_libraries(qplcore_${PLATFORM_ID} isal)
endforeach()

#
# Create dispatcher between platforms and auto-generated wrappers
#
file(GLOB SW_DISPATCHER_SOURCES ${QPL_SRC_DIR}/core-sw/dispatcher/*.cpp)

add_library(qplcore_sw_dispatcher OBJECT ${SW_DISPATCHER_SOURCES})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:qplcore_sw_dispatcher>)

target_include_directories(qplcore_sw_dispatcher
                          PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/dispatcher>)

# Generate kernel wrappers
generate_unpack_kernel_arrays(${QPL_BINARY_DIR} "${PLATFORMS_LIST}")

foreach(PLATFORM_ID IN LISTS PLATFORMS_LIST)
    file(GLOB GENERATED_${PLATFORM_ID}_TABLES_SRC ${QPL_BINARY_DIR}/generated/${PLATFORM_ID}_*.cpp)

    target_sources(qplcore_sw_dispatcher PRIVATE ${GENERATED_${PLATFORM_ID}_TABLES_SRC})

    # Set specific compiler options and/or definitions based on a platform
    if (${PLATFORM_ID} MATCHES "avx512")
        set_source_files_properties(${GENERATED_${PLATFORM_ID}_TABLES_SRC} PROPERTIES COMPILE_DEFINITIONS PLATFORM=2)
    else()
        set_source_files_properties(${GENERATED_${PLATFORM_ID}_TABLES_SRC} PROPERTIES COMPILE_DEFINITIONS PLATFORM=0)
    endif()

    target_include_directories(qplcore_sw_dispatcher
                                PUBLIC $<TARGET_PROPERTY:qplcore_${PLATFORM_ID},INTERFACE_INCLUDE_DIRECTORIES>)
endforeach()

set_target_properties(qplcore_sw_dispatcher PROPERTIES CXX_STANDARD 17)

# w/a for build compatibility with ISAL codebase
target_compile_definitions(qplcore_sw_dispatcher PUBLIC -DQPL_LIB)

target_compile_options(qplcore_sw_dispatcher
        PRIVATE ${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS})

# [SUBDIR]core-iaa
file(GLOB HW_PATH_SRC ${QPL_SRC_DIR}/core-iaa/sources/aecs/*.c
                      ${QPL_SRC_DIR}/core-iaa/sources/driver_loader/*.c
                      ${QPL_SRC_DIR}/core-iaa/sources/descriptors/*.c
                      ${QPL_SRC_DIR}/core-iaa/sources/*.c)

# Create library
add_library(core_iaa OBJECT ${HW_PATH_SRC})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:core_iaa>)

target_include_directories(core_iaa
        PRIVATE ${UUID_DIR}
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-iaa/include>
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-iaa/sources/include>
        PRIVATE $<BUILD_INTERFACE:${QPL_PROJECT_DIR}/include> # status.h in own_checkers.h
        PRIVATE $<TARGET_PROPERTY:qpl_c_api,INTERFACE_INCLUDE_DIRECTORIES> # for own_checkers.h
        PRIVATE $<TARGET_PROPERTY:qplcore_sw_dispatcher,INTERFACE_INCLUDE_DIRECTORIES>)

target_compile_features(core_iaa PRIVATE c_std_11)

target_compile_definitions(core_iaa PRIVATE QPL_BADARG_CHECK
        PRIVATE $<$<BOOL:${LOG_HW_INIT}>:LOG_HW_INIT>
        PRIVATE $<$<BOOL:${DYNAMIC_LOADING_LIBACCEL_CONFIG}>:DYNAMIC_LOADING_LIBACCEL_CONFIG>)

# [SUBDIR]middle-layer
file(GLOB MIDDLE_LAYER_SRC
        ${QPL_SRC_DIR}/middle-layer/accelerator/*.cpp
	    ${QPL_SRC_DIR}/middle-layer/analytics/*.cpp
        ${QPL_SRC_DIR}/middle-layer/common/*.cpp
        ${QPL_SRC_DIR}/middle-layer/compression/*.cpp
        ${QPL_SRC_DIR}/middle-layer/compression/*/*.cpp
        ${QPL_SRC_DIR}/middle-layer/compression/*/*/*.cpp
        ${QPL_SRC_DIR}/middle-layer/dispatcher/*.cpp
        ${QPL_SRC_DIR}/middle-layer/other/*.cpp
        ${QPL_SRC_DIR}/middle-layer/util/*.cpp)

add_library(middle_layer_lib OBJECT
        ${MIDDLE_LAYER_SRC})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
        $<TARGET_OBJECTS:middle_layer_lib>)

target_compile_options(middle_layer_lib
        PRIVATE $<$<C_COMPILER_ID:GNU,Clang>:$<$<CONFIG:Release>:-O3;-U_FORTIFY_SOURCE;-D_FORTIFY_SOURCE=2>>
        PRIVATE ${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS})

target_compile_definitions(middle_layer_lib
        PUBLIC QPL_VERSION="${QPL_VERSION}"
        PUBLIC $<$<BOOL:${LOG_HW_INIT}>:LOG_HW_INIT>
        PUBLIC $<$<BOOL:${EFFICIENT_WAIT}>:QPL_EFFICIENT_WAIT>
        PUBLIC QPL_BADARG_CHECK
        PUBLIC $<$<BOOL:${DYNAMIC_LOADING_LIBACCEL_CONFIG}>:DYNAMIC_LOADING_LIBACCEL_CONFIG>)

set_target_properties(middle_layer_lib PROPERTIES CXX_STANDARD 17)

target_include_directories(middle_layer_lib
        PRIVATE ${UUID_DIR}
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/middle-layer>
        PUBLIC $<TARGET_PROPERTY:_qpl,INTERFACE_INCLUDE_DIRECTORIES>
        PRIVATE $<TARGET_PROPERTY:qpl_c_api,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:qplcore_sw_dispatcher,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:isal,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:core_iaa,INTERFACE_INCLUDE_DIRECTORIES>)

target_compile_definitions(middle_layer_lib PUBLIC -DQPL_LIB)

# [SUBDIR]c_api
file(GLOB QPL_C_API_SRC
        ${QPL_SRC_DIR}/c_api/compression_operations/*.c
        ${QPL_SRC_DIR}/c_api/compression_operations/*.cpp
	    ${QPL_SRC_DIR}/c_api/filter_operations/*.cpp
	    ${QPL_SRC_DIR}/c_api/legacy_hw_path/*.c
	    ${QPL_SRC_DIR}/c_api/legacy_hw_path/*.cpp
	    ${QPL_SRC_DIR}/c_api/other_operations/*.cpp
	    ${QPL_SRC_DIR}/c_api/serialization/*.cpp
	    ${QPL_SRC_DIR}/c_api/*.cpp)

add_library(qpl_c_api OBJECT ${QPL_C_API_SRC})

target_include_directories(qpl_c_api
	    PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/c_api/>
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/include/> $<INSTALL_INTERFACE:include>
        PRIVATE $<TARGET_PROPERTY:middle_layer_lib,INTERFACE_INCLUDE_DIRECTORIES>)

set_target_properties(qpl_c_api PROPERTIES
	$<$<C_COMPILER_ID:GNU,Clang>:C_STANDARD 17
	CXX_STANDARD 17)

target_compile_options(qpl_c_api
        PRIVATE $<$<C_COMPILER_ID:GNU,Clang>:$<$<CONFIG:Release>:-O3;-U_FORTIFY_SOURCE;-D_FORTIFY_SOURCE=2>>
	    PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,GNU,Clang>:${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>)

target_compile_definitions(qpl_c_api
        PUBLIC -DQPL_BADARG_CHECK # own_checkers.h
        PUBLIC -DQPL_LIB          # needed for middle_layer_lib
        PUBLIC $<$<BOOL:${LOG_HW_INIT}>:LOG_HW_INIT>) # needed for middle_layer_lib

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
        $<TARGET_OBJECTS:qpl_c_api>)

# Final _qpl target

get_property(LIB_DEPS GLOBAL PROPERTY QPL_LIB_DEPS)

add_library(_qpl STATIC ${LIB_DEPS})

target_include_directories(_qpl
        PUBLIC $<BUILD_INTERFACE:${QPL_PROJECT_DIR}/include/> $<INSTALL_INTERFACE:include>)


target_compile_definitions(_qpl
        PUBLIC -DENABLE_QPL_COMPRESSION)

target_link_libraries(_qpl
        PRIVATE ch_contrib::accel-config)

target_include_directories(_qpl SYSTEM BEFORE
        PUBLIC "${QPL_PROJECT_DIR}/include"
        PUBLIC ${UUID_DIR})

add_library (ch_contrib::qpl ALIAS _qpl)
