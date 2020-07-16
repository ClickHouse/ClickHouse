option(ENABLE_PROTOBUF "Enable protobuf" ${ENABLE_LIBRARIES})

if(ENABLE_PROTOBUF)

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

    set(Protobuf_PROTOC_EXECUTABLE "$<TARGET_FILE:protoc>")
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

include (${ClickHouse_SOURCE_DIR}/cmake/protobuf_generate_cpp.cmake)
endif()

message(STATUS "Using protobuf=${USE_PROTOBUF}: ${Protobuf_INCLUDE_DIR} : ${Protobuf_LIBRARY} : ${Protobuf_PROTOC_EXECUTABLE}")
