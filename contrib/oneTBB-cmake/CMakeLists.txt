set(oneTBB_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/oneTBB-cmake")
set(oneTBB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/oneTBB")

set(BUILD_SHARED_LIBS OFF CACHE INTERNAL "" FORCE) # actually will use zlib, but skip find
set(CMAKE_CXX_STANDARD 20 CACHE INTERNAL "" FORCE)
set(TBBMALLOC_BUILD OFF CACHE INTERNAL "" FORCE)
set(TBB_OUTPUT_DIR_BASE build CACHE INTERNAL "" FORCE)

add_subdirectory("${oneTBB_SOURCE_DIR}" "${oneTBB_LIBRARY_DIR}")

#add_library(${SRCS})

#if (CMAKE_CROSSCOMPILING)
#    # Build 'protoc' for host arch
#    set (PROTOC_BUILD_DIR "${protobuf_BINARY_DIR}/build")
#
#    if (NOT EXISTS "${PROTOC_BUILD_DIR}/protoc")
#
#        # This is quite ugly but I cannot make dependencies work propery.
#
#        execute_process(
#            COMMAND mkdir -p ${PROTOC_BUILD_DIR}
#            COMMAND_ECHO STDOUT)
#
#        execute_process(
#            COMMAND ${CMAKE_COMMAND}
#                "-G${CMAKE_GENERATOR}"
#                "-DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
#                "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
#                "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
#                "-Dprotobuf_BUILD_TESTS=0"
#                "-Dprotobuf_BUILD_CONFORMANCE=0"
#                "-Dprotobuf_BUILD_EXAMPLES=0"
#                "-Dprotobuf_BUILD_PROTOC_BINARIES=1"
#                "${protobuf_SOURCE_DIR}/cmake"
#            WORKING_DIRECTORY "${PROTOC_BUILD_DIR}"
#            COMMAND_ECHO STDOUT)
#
#        execute_process(
#            COMMAND ${CMAKE_COMMAND} --build "${PROTOC_BUILD_DIR}"
#            COMMAND_ECHO STDOUT)
#    endif ()
#
#    add_executable(protoc IMPORTED GLOBAL)
#    set_target_properties (protoc PROPERTIES IMPORTED_LOCATION "${PROTOC_BUILD_DIR}/protoc")
#    add_dependencies(protoc "${PROTOC_BUILD_DIR}/protoc")
#
#endif ()
