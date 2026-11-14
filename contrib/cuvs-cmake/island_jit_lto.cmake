# librtcx's JIT-LTO fragment generator, redefined for the GPU island.

include ("${ClickHouse_SOURCE_DIR}/contrib/librtcx/cmake/modules/generate_jit_lto_kernels.cmake")

set (CUVS_REGISTER_FATBIN_TEMPLATE "${CMAKE_CURRENT_LIST_DIR}/register_fatbin.cpp.in")
set (CUVS_BIN2C "${GPU_CUDA_ROOT}/bin/bin2c")
if (NOT EXISTS "${CUVS_BIN2C}")
    message (FATAL_ERROR "bin2c is missing from the CUDA toolkit at ${GPU_CUDA_ROOT}")
endif ()

function (add_jit_lto_kernel kernel_target)
    cmake_parse_arguments (_JIT_LTO "" "KERNEL_FILE;FATBIN_HEADER_FILE" "LINK_LIBRARIES;EXTRA_COMPILE_OPTIONS" ${ARGN})

    add_library (${kernel_target} OBJECT EXCLUDE_FROM_ALL "${_JIT_LTO_KERNEL_FILE}")
    gpu_island_target (${kernel_target})

    target_link_libraries (${kernel_target} PRIVATE ${_JIT_LTO_LINK_LIBRARIES})
    target_compile_options (${kernel_target} PRIVATE ${CUVS_CUDA_FLAGS} -Xfatbin=--compress-all --compress-mode=size ${_JIT_LTO_EXTRA_COMPILE_OPTIONS})
    set_target_properties (${kernel_target} PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
        CUDA_FATBIN_COMPILATION ON
        POSITION_INDEPENDENT_CODE ON
        INTERPROCEDURAL_OPTIMIZATION ON
        CUDA_ARCHITECTURES ${JIT_LTO_TARGET_ARCHITECTURE})

    add_custom_command (OUTPUT "${_JIT_LTO_FATBIN_HEADER_FILE}"
        COMMAND "${CUVS_BIN2C}" --const --name embedded_fatbin --static $<TARGET_OBJECTS:${kernel_target}> > "${_JIT_LTO_FATBIN_HEADER_FILE}"
        DEPENDS $<TARGET_OBJECTS:${kernel_target}> ${kernel_target}
        COMMENT "Generating fatbin header file for ${kernel_target}")
endfunction ()

function (process_jit_lto_matrix_entry source_list_var)
    cmake_parse_arguments (_JIT_LTO ""
        "NAME_FORMAT;KERNEL_INPUT_FILE;OUTPUT_DIRECTORY;FRAGMENT_TAG_FORMAT;MATRIX_JSON_ENTRY"
        "KERNEL_LINK_LIBRARIES;FRAGMENT_TAG_HEADER_FILES;KERNEL_EXTRA_COMPILE_OPTIONS" ${ARGN})

    populate_matrix_variables ("${_JIT_LTO_MATRIX_JSON_ENTRY}")
    string (CONFIGURE "${_JIT_LTO_NAME_FORMAT}" kernel_name @ONLY)
    string (CONFIGURE "${_JIT_LTO_FRAGMENT_TAG_FORMAT}" fragment_tag @ONLY)

    set (fragment_tag_header_files "")
    foreach (header_file IN LISTS _JIT_LTO_FRAGMENT_TAG_HEADER_FILES)
        if (NOT header_file MATCHES "^(\".*\"|<.*>)$")
            set (header_file "\"${header_file}\"")
        endif ()
        string (APPEND fragment_tag_header_files "#include ${header_file}\n")
    endforeach ()

    set (kernel_file "${_JIT_LTO_OUTPUT_DIRECTORY}/${kernel_name}_kernel.cu")
    set (kernel_target "${kernel_name}_kernel")
    set (fatbin_header_file "${_JIT_LTO_OUTPUT_DIRECTORY}/${kernel_name}_fatbin.h")
    set (fatbin_file "${_JIT_LTO_OUTPUT_DIRECTORY}/${kernel_name}_fatbin.cpp")

    configure_file ("${_JIT_LTO_KERNEL_INPUT_FILE}" "${kernel_file}" @ONLY)
    configure_file ("${CUVS_REGISTER_FATBIN_TEMPLATE}" "${fatbin_file}" @ONLY)
    add_jit_lto_kernel (${kernel_target}
        KERNEL_FILE "${kernel_file}"
        FATBIN_HEADER_FILE "${fatbin_header_file}"
        LINK_LIBRARIES ${_JIT_LTO_KERNEL_LINK_LIBRARIES}
        EXTRA_COMPILE_OPTIONS ${_JIT_LTO_KERNEL_EXTRA_COMPILE_OPTIONS})

    list (APPEND ${source_list_var} "${fatbin_header_file}" "${fatbin_file}")
    set (${source_list_var} "${${source_list_var}}" PARENT_SCOPE)
endfunction ()

function (generate_jit_lto_kernels source_list_var)
    cmake_parse_arguments (_JIT_LTO ""
        "NAME_FORMAT;MATRIX_JSON_FILE;MATRIX_JSON_STRING;KERNEL_INPUT_FILE;FRAGMENT_TAG_FORMAT;OUTPUT_DIRECTORY"
        "KERNEL_LINK_LIBRARIES;FRAGMENT_TAG_HEADER_FILES;KERNEL_EXTRA_COMPILE_OPTIONS" ${ARGN})

    if (_JIT_LTO_MATRIX_JSON_FILE)
        set_property (DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_JIT_LTO_MATRIX_JSON_FILE}")
        compute_matrix_product (matrix_product MATRIX_JSON_FILE "${_JIT_LTO_MATRIX_JSON_FILE}")
    else ()
        compute_matrix_product (matrix_product MATRIX_JSON_STRING "${_JIT_LTO_MATRIX_JSON_STRING}")
    endif ()

    string (JSON len LENGTH "${matrix_product}")
    math (EXPR last "${len} - 1")
    foreach (i RANGE "${last}")
        string (JSON matrix_json_entry GET "${matrix_product}" "${i}")
        process_jit_lto_matrix_entry ("${source_list_var}"
            NAME_FORMAT "${_JIT_LTO_NAME_FORMAT}"
            KERNEL_INPUT_FILE "${_JIT_LTO_KERNEL_INPUT_FILE}"
            FRAGMENT_TAG_FORMAT "${_JIT_LTO_FRAGMENT_TAG_FORMAT}"
            FRAGMENT_TAG_HEADER_FILES ${_JIT_LTO_FRAGMENT_TAG_HEADER_FILES}
            OUTPUT_DIRECTORY "${_JIT_LTO_OUTPUT_DIRECTORY}"
            MATRIX_JSON_ENTRY "${matrix_json_entry}"
            KERNEL_LINK_LIBRARIES ${_JIT_LTO_KERNEL_LINK_LIBRARIES}
            KERNEL_EXTRA_COMPILE_OPTIONS ${_JIT_LTO_KERNEL_EXTRA_COMPILE_OPTIONS})
    endforeach ()
    set (${source_list_var} "${${source_list_var}}" PARENT_SCOPE)
endfunction ()
