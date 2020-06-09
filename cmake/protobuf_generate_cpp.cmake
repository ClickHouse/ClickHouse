# This file declares functions adding custom commands for generating C++ files from *.proto files:
# function (protobuf_generate_cpp SRCS HDRS)
# function (protobuf_generate_grpc_cpp SRCS HDRS)

if (NOT USE_PROTOBUF)
    message (WARNING "Could not use protobuf_generate_cpp() without the protobuf library")
    return()
endif()

if (NOT DEFINED PROTOBUF_PROTOC_EXECUTABLE)
    set (PROTOBUF_PROTOC_EXECUTABLE "$<TARGET_FILE:protoc>")
endif()

if (NOT DEFINED GRPC_CPP_PLUGIN_EXECUTABLE)
    set (GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
endif()

if (NOT DEFINED PROTOBUF_GENERATE_CPP_APPEND_PATH)
    set (PROTOBUF_GENERATE_CPP_APPEND_PATH TRUE)
endif()


function(protobuf_generate_cpp_impl SRCS HDRS MODES OUTPUT_FILE_EXTS PLUGIN)
    if(NOT ARGN)
        message(SEND_ERROR "Error: protobuf_generate_cpp() called without any proto files")
        return()
    endif()

    if(PROTOBUF_GENERATE_CPP_APPEND_PATH)
        # Create an include path for each file specified
        foreach(FIL ${ARGN})
            get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
            get_filename_component(ABS_PATH ${ABS_FIL} PATH)
            list(FIND protobuf_include_path ${ABS_PATH} _contains_already)
            if(${_contains_already} EQUAL -1)
                list(APPEND protobuf_include_path -I ${ABS_PATH})
            endif()
        endforeach()
    else()
        set(protobuf_include_path -I ${CMAKE_CURRENT_SOURCE_DIR})
    endif()

    if(DEFINED PROTOBUF_IMPORT_DIRS AND NOT DEFINED Protobuf_IMPORT_DIRS)
        set(Protobuf_IMPORT_DIRS "${PROTOBUF_IMPORT_DIRS}")
    endif()

    if(DEFINED Protobuf_IMPORT_DIRS)
        foreach(DIR ${Protobuf_IMPORT_DIRS})
            get_filename_component(ABS_PATH ${DIR} ABSOLUTE)
            list(FIND protobuf_include_path ${ABS_PATH} _contains_already)
            if(${_contains_already} EQUAL -1)
                list(APPEND protobuf_include_path -I ${ABS_PATH})
            endif()
        endforeach()
    endif()

    set (intermediate_dir ${CMAKE_CURRENT_BINARY_DIR}/intermediate)
    file (MAKE_DIRECTORY ${intermediate_dir})

    set (protoc_args)
    foreach (mode ${MODES})
        list (APPEND protoc_args "--${mode}_out" ${intermediate_dir})
    endforeach()
    if (PLUGIN)
        list (APPEND protoc_args "--plugin=${PLUGIN}")
    endif()

    set(srcs)
    set(hdrs)
    set(all_intermediate_outputs)

    foreach(input_name ${ARGN})
        get_filename_component(abs_name ${input_name} ABSOLUTE)
        get_filename_component(name ${input_name} NAME_WE)
        
        set (intermediate_outputs)
        foreach (ext ${OUTPUT_FILE_EXTS})
            set (filename "${name}${ext}")
            set (output "${CMAKE_CURRENT_BINARY_DIR}/${filename}")
            set (intermediate_output "${intermediate_dir}/${filename}")
            list (APPEND intermediate_outputs "${intermediate_output}")
            list (APPEND all_intermediate_outputs "${intermediate_output}")

            if (${ext} MATCHES ".*\\.h")
                list(APPEND hdrs "${output}")
            else()
                list(APPEND srcs "${output}")
            endif()

            add_custom_command(
                OUTPUT ${output}
                COMMAND ${CMAKE_COMMAND} -DPROTOBUF_GENERATE_CPP_SCRIPT_MODE=1 -DUSE_PROTOBUF=1 -DDIR=${CMAKE_CURRENT_BINARY_DIR} -DFILENAME=${filename} -DCOMPILER_ID=${CMAKE_CXX_COMPILER_ID} -P ${ClickHouse_SOURCE_DIR}/cmake/protobuf_generate_cpp.cmake
                DEPENDS ${intermediate_output})
        endforeach()

        add_custom_command(
            OUTPUT ${intermediate_outputs}
            COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            ARGS ${protobuf_include_path} ${protoc_args} ${abs_name}
            DEPENDS ${abs_name} ${Protobuf_PROTOC_EXECUTABLE} ${PLUGIN}
            COMMENT "Running C++ protocol buffer compiler on ${name}"
            VERBATIM )
    endforeach()

    set_source_files_properties(${srcs} ${hdrs} ${all_intermediate_outputs} PROPERTIES GENERATED TRUE)
    set(${SRCS} ${srcs} PARENT_SCOPE)
    set(${HDRS} ${hdrs} PARENT_SCOPE)
endfunction()


if (PROTOBUF_GENERATE_CPP_SCRIPT_MODE)
    set (output "${DIR}/${FILENAME}")
    set (intermediate_dir ${DIR}/intermediate)
    set (intermediate_output "${intermediate_dir}/${FILENAME}")

    if (COMPILER_ID MATCHES "Clang")
        set (pragma_push "#pragma clang diagnostic push\n")
        set (pragma_pop "#pragma clang diagnostic pop\n")
        set (pragma_disable_warnings "#pragma clang diagnostic ignored \"-Weverything\"\n")
    elseif (COMPILER_ID MATCHES "GNU")
        set (pragma_push "#pragma GCC diagnostic push\n")
        set (pragma_pop "#pragma GCC diagnostic pop\n")
        set (pragma_disable_warnings "#pragma GCC diagnostic ignored \"-Wall\"\n"
                                     "#pragma GCC diagnostic ignored \"-Wextra\"\n"
                                     "#pragma GCC diagnostic ignored \"-Warray-bounds\"\n"
                                     "#pragma GCC diagnostic ignored \"-Wold-style-cast\"\n"
                                     "#pragma GCC diagnostic ignored \"-Wshadow\"\n"
                                     "#pragma GCC diagnostic ignored \"-Wsuggest-override\"\n"
                                     "#pragma GCC diagnostic ignored \"-Wcast-qual\"\n"
                                     "#pragma GCC diagnostic ignored \"-Wunused-parameter\"\n")
    endif()

    if (${FILENAME} MATCHES ".*\\.h")
        file(WRITE "${output}"
            "#pragma once\n"
            ${pragma_push}
            ${pragma_disable_warnings}
            "#include \"${intermediate_output}\"\n"
            ${pragma_pop}
        )
    else()
        file(WRITE "${output}"
            ${pragma_disable_warnings}
            "#include \"${intermediate_output}\"\n"
        )
    endif()
    return()
endif()


function(protobuf_generate_cpp SRCS HDRS)
    set (modes cpp)
    set (output_file_exts ".pb.cc" ".pb.h")
    set (plugin)

    protobuf_generate_cpp_impl(srcs hdrs "${modes}" "${output_file_exts}" "${plugin}" ${ARGN})

    set(${SRCS} ${srcs} PARENT_SCOPE)
    set(${HDRS} ${hdrs} PARENT_SCOPE)
endfunction()


function(protobuf_generate_grpc_cpp SRCS HDRS)
    set (modes cpp grpc)
    set (output_file_exts ".pb.cc" ".pb.h" ".grpc.pb.cc" ".grpc.pb.h")
    set (plugin "protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}")

    protobuf_generate_cpp_impl(srcs hdrs "${modes}" "${output_file_exts}" "${plugin}" ${ARGN})

    set(${SRCS} ${srcs} PARENT_SCOPE)
    set(${HDRS} ${hdrs} PARENT_SCOPE)
endfunction()
