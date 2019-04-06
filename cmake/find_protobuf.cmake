option (ENABLE_PROTOBUF "Enable protobuf" ON)

if (ENABLE_PROTOBUF)

option(USE_INTERNAL_PROTOBUF_LIBRARY "Set to FALSE to use system protobuf instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/protobuf/cmake/CMakeLists.txt")
   if(USE_INTERNAL_PROTOBUF_LIBRARY)
       message(WARNING "submodule contrib/protobuf is missing. to fix try run: \n git submodule update --init --recursive")
       set(USE_INTERNAL_PROTOBUF_LIBRARY 0)
   endif()
   set(MISSING_INTERNAL_PROTOBUF_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_PROTOBUF_LIBRARY)
    find_package(Protobuf)
endif()

if (Protobuf_LIBRARY AND Protobuf_INCLUDE_DIR)
    set(USE_PROTOBUF 1)
elseif(NOT MISSING_INTERNAL_PROTOBUF_LIBRARY)
    set(Protobuf_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/protobuf/src)

    set(USE_PROTOBUF 1)
    set(USE_INTERNAL_PROTOBUF_LIBRARY 1)
    set(Protobuf_LIBRARY libprotobuf)
    set(Protobuf_PROTOC_LIBRARY libprotoc)
    set(Protobuf_LITE_LIBRARY libprotobuf-lite)

    set(Protobuf_PROTOC_EXECUTABLE ${ClickHouse_BINARY_DIR}/contrib/protobuf/cmake/protoc)

    if(NOT DEFINED PROTOBUF_GENERATE_CPP_APPEND_PATH)
        set(PROTOBUF_GENERATE_CPP_APPEND_PATH TRUE)
    endif()

    function(PROTOBUF_GENERATE_CPP SRCS HDRS)
        if(NOT ARGN)
            message(SEND_ERROR "Error: PROTOBUF_GENERATE_CPP() called without any proto files")
            return()
        endif()

        if(PROTOBUF_GENERATE_CPP_APPEND_PATH)
            # Create an include path for each file specified
            foreach(FIL ${ARGN})
                get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
                get_filename_component(ABS_PATH ${ABS_FIL} PATH)
                list(FIND _protobuf_include_path ${ABS_PATH} _contains_already)
                if(${_contains_already} EQUAL -1)
                    list(APPEND _protobuf_include_path -I ${ABS_PATH})
                endif()
            endforeach()
        else()
            set(_protobuf_include_path -I ${CMAKE_CURRENT_SOURCE_DIR})
        endif()

        if(DEFINED PROTOBUF_IMPORT_DIRS AND NOT DEFINED Protobuf_IMPORT_DIRS)
            set(Protobuf_IMPORT_DIRS "${PROTOBUF_IMPORT_DIRS}")
        endif()

        if(DEFINED Protobuf_IMPORT_DIRS)
            foreach(DIR ${Protobuf_IMPORT_DIRS})
                get_filename_component(ABS_PATH ${DIR} ABSOLUTE)
                list(FIND _protobuf_include_path ${ABS_PATH} _contains_already)
                if(${_contains_already} EQUAL -1)
                    list(APPEND _protobuf_include_path -I ${ABS_PATH})
                endif()
            endforeach()
        endif()

        set(${SRCS})
        set(${HDRS})
        foreach(FIL ${ARGN})
            get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
            get_filename_component(FIL_WE ${FIL} NAME_WE)

            list(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc")
            list(APPEND ${HDRS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h")

            add_custom_command(
                OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc"
                "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h"
                COMMAND  ${Protobuf_PROTOC_EXECUTABLE}
                ARGS --cpp_out  ${CMAKE_CURRENT_BINARY_DIR} ${_protobuf_include_path} ${ABS_FIL}
                DEPENDS ${ABS_FIL} ${Protobuf_PROTOC_EXECUTABLE}
                COMMENT "Running C++ protocol buffer compiler on ${FIL}"
                VERBATIM )
        endforeach()

        set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
        set(${SRCS} ${${SRCS}} PARENT_SCOPE)
        set(${HDRS} ${${HDRS}} PARENT_SCOPE)
    endfunction()
endif()

if(OS_FREEBSD AND SANITIZE STREQUAL "address")
    # ../contrib/protobuf/src/google/protobuf/arena_impl.h:45:10: fatal error: 'sanitizer/asan_interface.h' file not found
    # #include <sanitizer/asan_interface.h>
    if(LLVM_INCLUDE_DIRS)
        set(Protobuf_INCLUDE_DIR ${Protobuf_INCLUDE_DIR} ${LLVM_INCLUDE_DIRS})
    else()
        set(USE_PROTOBUF 0)
    endif()
endif()

endif()

message(STATUS "Using protobuf=${USE_PROTOBUF}: ${Protobuf_INCLUDE_DIR} : ${Protobuf_LIBRARY}")
